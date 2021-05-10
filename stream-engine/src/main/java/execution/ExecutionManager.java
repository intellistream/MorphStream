package execution;
import common.Clock;
import common.collections.Configuration;
import components.context.TopologyContext;
import components.exception.UnhandledCaseException;
import controller.affinity.AffinityController;
import db.Database;
import execution.runtime.boltThread;
import execution.runtime.executorThread;
import execution.runtime.spoutThread;
import faulttolerance.Writer;
import optimization.OptimizationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.dedicated.ordered.TxnProcessingEngine;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static common.CONTROL.enable_shared_state;
import static common.Constants.*;
/**
 * Created by shuhaozhang on 19/8/16.
 */
public class ExecutionManager {
    private final static Logger LOG = LoggerFactory.getLogger(ExecutionManager.class);
    public static Clock clock = null;
    public final HashMap<Integer, executorThread> ThreadMap = new HashMap<>();
    public final AffinityController AC;
    private final OptimizationManager optimizationManager;
    TxnProcessingEngine tp_engine;
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
    public void distributeTasks(Configuration conf,
                                CountDownLatch latch, Database db) throws UnhandledCaseException {
        g.build_inputScheduler();
        clock = new Clock(conf.getDouble("checkpoint", 1));
        if (conf.getBoolean("enable_fault_tolerance", false)) {
            Writer writer = null;
            for (ExecutionNode e : g.getExecutionNodeArrayList()) {
                if (e.isFirst_executor()) {
                    writer = new Writer(e.operator, e.operator.getNumTasks());
                }
                e.configureWriter(writer);
            }
        }
        //TODO: support multi-stages later.
        if (enable_shared_state) {
            HashMap<Integer, List<Integer>> stage_map = new HashMap<>();//Stages --> Executors.
            for (ExecutionNode e : g.getExecutionNodeArrayList()) {
                stage_map.putIfAbsent(e.op.getStage(), new LinkedList<>());
                stage_map.get(e.op.getStage()).add(e.getExecutorID());
            }
            int stage = 0;//currently only stage 0 is required..
            List<Integer> integers = stage_map.get(stage);
//            TxnProcessingEngine tp_engine = new TxnProcessingEngine(stage);
            tp_engine = TxnProcessingEngine.getInstance();
            if (integers != null) {
                tp_engine.initilize(integers.size(), conf.getInt("app"));//TODO: use fixed number of partition?
                tp_engine.engine_init(integers.get(0), integers.get(integers.size() - 1), integers.size(), conf.getInt("TP", 10), conf.getString("scheduler", "BL"));
            }
        }
        executorThread thread = null;
        long start = System.currentTimeMillis();
        for (ExecutionNode e : g.getExecutionNodeArrayList()) {
            switch (e.operator.type) {
                case spoutType:
                    thread = launchSpout_SingleCore(e, new TopologyContext(g, db, e, ThreadMap)
                            , conf, 0, latch); //TODO: schedule to numa node wisely.
                    break;
                case boltType:
                case sinkType:
                    thread = launchBolt_SingleCore(e, new TopologyContext(g, db, e, ThreadMap)
                            , conf, 0, latch); //TODO: schedule to numa node wisely.
                    break;
                case virtualType:
                    LOG.info("Won't launch virtual ground");
                    break;
                default:
                    throw new UnhandledCaseException("type not recognized");
            }
//                if (previous_op == null || e.operator != previous_op) {
            if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
                assert thread != null;
                while (!thread.isReady()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
//                            ex.printStackTrace();
                    }
                }
            }
        }
        long end = System.currentTimeMillis();
        LOG.info("It takes :" + (end - start) / 1000 + " seconds to finish launch the operators.");
    }
    private executorThread launchSpout_InCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                              int node, long[] cores, CountDownLatch latch) {

        spoutThread st;
        st = new spoutThread(e, context, conf, cores, node, latch,
                ThreadMap, clock);
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
                optimizationManager, ThreadMap, clock);
        wt.setDaemon(true);
        if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
            wt.start();
        }
        ThreadMap.putIfAbsent(e.getExecutorID(), wt);
        return wt;
    }
    private executorThread launchSpout_SingleCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                                  int node, CountDownLatch latch) {
        LOG.info("Launch Spout:" + e.getOP() + " on node:" + node);
        long[] cpu;
        if (!conf.getBoolean("NAV", true)) {
            cpu = AC.requirePerCore(node);
        } else {
            cpu = new long[1];
        }

//		LOG.info("Launch spout on cpu:" + Arrays.show(cpu));
        return launchSpout_InCore(e, context, conf, node, cpu, latch);
    }
    private executorThread launchBolt_SingleCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                                 int node, CountDownLatch latch) {
        LOG.info("Launch bolt:" + e.getOP() + " on node:" + node);
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
        LOG.info("Execution stops.");
        if (clock != null) {
            clock.close();
        }
        this.getSinkThread().getContext().Sequential_stopAll();
        if (enable_shared_state && tp_engine != null)
            tp_engine.engine_shutdown();
    }
    public executorThread getSinkThread() {
        return ThreadMap.get(g.getSinkThread());
    }
}
