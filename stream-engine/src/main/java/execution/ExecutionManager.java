package execution;

import common.collections.Configuration;
import components.context.TopologyContext;
import components.exception.UnhandledCaseException;
import controller.affinity.AffinityController;
import db.Database;
import execution.runtime.boltThread;
import execution.runtime.executorThread;
import execution.runtime.spoutThread;
import optimization.OptimizationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stage.Stage;
import stage.StageManager;
import transaction.TxnManager;
import utils.AppConfig;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_shared_state;
import static common.Constants.*;

/**
 * Created by shuhaozhang on 19/8/16.
 */
public class ExecutionManager {
    private final static Logger LOG = LoggerFactory.getLogger(ExecutionManager.class);
    public final HashMap<Integer, executorThread> ThreadMap = new HashMap<>();
    public final HashMap<Integer, Stage> StageMap = new HashMap<>();
    public final AffinityController AC;
    private final OptimizationManager optimizationManager;
    private final ExecutionGraph g;

    public ExecutionManager(ExecutionGraph g, Configuration conf, OptimizationManager optimizationManager) {
        this.g = g;
        AC = new AffinityController(conf);
        this.optimizationManager = optimizationManager;

    }

    /**
     * Launch threads for each executor in executionGraph
     * We make sure no interference among threads --> one thread one core.
     * TODO: let's think about how to due with multi-thread per core in future..
     * All executors have to sync_ratio for OM to start, so it's safe to do initialization here. E.g., initialize database.
     */
    public void distributeTasks(Configuration conf, CountDownLatch latch, Database db) throws UnhandledCaseException {
        g.build_inputScheduler();
        if (enable_shared_state) {
            createStages(conf);
        }

        executorThread thread = null;
        long start = System.currentTimeMillis();
        for (ExecutionNode e : g.getExecutionNodeArrayList()) {
            switch (e.operator.type) {
                case spoutType:
                    thread = launchSpout_SingleCore(e, new TopologyContext(g, db, e, ThreadMap, StageMap)
                            , conf, 0, latch); //TODO: schedule to numa node wisely.
                    break;
                case boltType:
                    thread = launchBolt_SingleCore(e, new TopologyContext(g, db, e, ThreadMap, StageMap)
                            , conf, 0, latch); //TODO: schedule to numa node wisely.
                    break;
                case sinkType:
                    thread = launchBolt_SingleCore(e, new TopologyContext(g, db, e, ThreadMap, StageMap)
                            , conf, 0, latch); //TODO: schedule to numa node wisely.
                    break;
                case virtualType:
                    if (enable_log) LOG.info("Won't launch virtual ground");
                    break;
                default:
                    throw new UnhandledCaseException("type not recognized");
            }
            if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
                assert thread != null;
                while (!thread.isReady()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
        long end = System.currentTimeMillis();
        if (enable_log) LOG.info("It takes :" + (end - start) / 1000 + " seconds to finish launch the operators.");
    }

    private void createStages(Configuration conf) {
        HashMap<Integer, List<Integer>> stage_map = new HashMap<>();//Stages --> Executors.
        for (ExecutionNode e : g.getExecutionNodeArrayList()) {
            stage_map.putIfAbsent(e.op.getStage(), new LinkedList<>());
            stage_map.get(e.op.getStage()).add(e.getExecutorID());
        }
        int stage = 0;
        List<Integer> integers;

        /**
         * Create scheduler and control for each stage.
         * One stage one scheduler.
         */
        do {
            integers = stage_map.get(stage);
            if (integers == null) break;
            StageMap.putIfAbsent(stage, new Stage());
            int totalThread = integers.size();//Number of threads per stage..
            int numberOfStates = conf.getInt("NUM_ITEMS");//TODO: Need to be stage aware..
            String schedulerType = conf.getString("scheduler");
            int app = conf.getInt("app");


            StageMap.get(stage).CreateController(totalThread);

            StageMap.get(stage).CreateScheduler(schedulerType, totalThread, numberOfStates, app);
            stage++;
        } while (true);
    }

    private executorThread launchSpout_InCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                              int node, long[] cores, CountDownLatch latch) {
        spoutThread st;
        st = new spoutThread(e, context, conf, cores, node, latch,
                ThreadMap);
        st.setDaemon(true);
        if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
            st.start();
        }
        ThreadMap.putIfAbsent(e.getExecutorID(), st);
        return st;
    }

    private executorThread launchBolt_InCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                             int node, long[] cores, CountDownLatch latch) {
        boltThread wt;
        wt = new boltThread(e, context, conf, cores, node, latch,
                optimizationManager, ThreadMap); //Initialize a boltThread using the input ExecutionNode
        wt.setDaemon(true);
        if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
            wt.start();
        }
        ThreadMap.putIfAbsent(e.getExecutorID(), wt);
        return wt;
    }

    private executorThread launchSpout_SingleCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                                  int node, CountDownLatch latch) {
        if (enable_log) LOG.info("Launch" + e.getOP() + " on node:" + node);
        long[] cpu;
        if (!conf.getBoolean("NAV", true)) {
            cpu = AC.requirePerCore(node);
        } else {
            cpu = new long[1];
        }

        return launchSpout_InCore(e, context, conf, node, cpu, latch);
    }

    private executorThread launchBolt_SingleCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                                 int node, CountDownLatch latch) {
        if (enable_log) LOG.info("Launch bolt:" + e.getOP() + " on node:" + node);
        long[] cpu;
        if (!conf.getBoolean("NAV", true)) {
            cpu = AC.requirePerCore(node);
        } else {
            cpu = new long[1];
        }
        return launchBolt_InCore(e, context, conf, node, cpu, latch);
    }

    /**
     * stop EM
     * It stops all execution threads as well.
     */
    public void exist() {
        if (enable_log) LOG.info("Execution stops.");
        this.getSinkThread().getContext().Sequential_stopAll();
    }

    public executorThread getSinkThread() {
        return ThreadMap.get(g.getSinkThread());
    }
}
