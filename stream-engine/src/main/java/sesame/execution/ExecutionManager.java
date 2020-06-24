package sesame.execution;

import application.CONTROL;
import application.Platform;
import application.util.Configuration;
import ch.usi.overseer.OverHpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.TopologyComponent;
import sesame.components.context.TopologyContext;
import sesame.components.exception.UnhandledCaseException;
import sesame.controller.affinity.AffinityController;
import sesame.execution.runtime.boltThread;
import sesame.execution.runtime.executorThread;
import sesame.execution.runtime.spoutThread;
import sesame.faulttolerance.Writer;
import sesame.optimization.ExecutionPlan;
import sesame.optimization.OptimizationManager;
import state_engine.Clock;
import state_engine.Database;
import state_engine.transaction.dedicated.ordered.TxnProcessingEngine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static application.Constants.EVENTS.*;
import static application.Constants.*;
import static application.util.OsUtils.isUnix;
import static xerial.jnuma.Numa.getAffinity;
import static xerial.jnuma.Numa.setAffinity;

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
    private int loadTargetHz;
    private int timeSliceLengthMs;
    private OverHpc HPCMonotor;
    private ExecutionGraph g;
    private boolean Txn_lock = true;

    public ExecutionManager(ExecutionGraph g, Configuration conf, OptimizationManager optimizationManager, Database db, Platform p) {
        this.g = g;
        AC = new AffinityController(conf, p);
        this.optimizationManager = optimizationManager;
        initializeHPC();
    }

    /**
     * CPU_CLK_UNHALTED.REF
     * MicroEvent Code: 0x00
     * <p>
     * Mask: 0x00
     * <p>
     * Category: Basic Events;All Events;
     * <p>
     * Definition: Reference cycles when core is not halted.
     * <p>
     * Description: This input_event counts the number of reference cycles that the core is not in a halt state. The core enters the halt state when it is running the HLT instruction.
     * In mobile systems the core frequency may change from time. This input_event is not affected by core frequency changes but counts as if the core is running at the maximum frequency all the time. This input_event has a constant ratio with the CPU_CLK_UNHALTED.BUS input_event.
     * Divide this input_event count by core frequency to determine the elapsed time while the core was not in halt state.
     * Note: The input_event CPU_CLK_UNHALTED.REF is counted by a designated fixed timestamp_counter, leaving the two programmable counters available for other events.
     */


    private void initializeHPC() {
        if (isUnix()) {
            try {
                HPCMonotor = OverHpc.getInstance();
                if (HPCMonotor == null) {
                    System.out.println("ERROR: unable to init OverHpc");
                }

                // Init input_event: LLC miss for memory fetch. + "," + LLC_PREFETCHES+ "," + L1_ICACHE_LOADS
                if (!HPCMonotor.initEvents(
                        LLC_MISSES
                                + "," + LLC_REFERENCES
                                + "," + PERF_COUNT_HW_CPU_CYCLES
//								+ "," + L1_ICACHE_LOAD_MISSES
//								+ "," + L1_DCACHE_LOAD_MISSES
                )) {
                    LOG.error("ERROR: invalid input_event");
                }
            } catch (java.lang.UnsatisfiedLinkError e) {
                System.out.println("ERROR: unable to init OverHpc. " + e.getMessage());
                HPCMonotor = null;
            }
        }
    }

    /**
     * Launch threads for each executor in executionGraph
     * We make sure no interference among threads --> one thread one core.
     * TODO: let's think about how to due with multi-thread per core in future..
     * All executors have to sync_ratio for OM to start, so it's safe to do initialization here. E.g., initialize database.
     */
    public void distributeTasks(Configuration conf,
                                ExecutionPlan plan, CountDownLatch latch, Database db, Platform p) throws UnhandledCaseException {
        assert plan != null;
        loadTargetHz = (int) conf.getDouble("targetHz", 10000000);
        LOG.info("Finally, targetHZ set to:" + loadTargetHz);
        timeSliceLengthMs = conf.getInt("timeSliceLengthMs");

        if (plan.getSP() != null) {
            this.g = plan.getSP().graph;
        }
        g.build_inputScheduler();
        clock = new Clock(conf.getDouble("checkpoint", 1));

        if (conf.getBoolean("Fault_tolerance", false)) {
            Writer writer = null;
            for (ExecutionNode e : g.getExecutionNodeArrayList()) {
                if (e.isFirst_executor()) {
                    writer = new Writer(e.operator, e.operator.getNumTasks());
                }
                e.configureWriter(writer);
            }
        }

        //TODO: support multi-stages later.
        if (conf.getBoolean("transaction", false)) {
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
                tp_engine.engine_init(integers.get(0), integers.get(integers.size() - 1), integers.size(), conf.getInt("TP", 10));
            }
        }

        executorThread thread = null;
        TopologyComponent previous_op = null;
        long start = System.currentTimeMillis();
        for (ExecutionNode e : g.getExecutionNodeArrayList()) {

            switch (e.operator.type) {
                case spoutType:
                    thread = launchSpout_SingleCore(e, new TopologyContext(g, db, plan, e, ThreadMap, HPCMonotor)
                            , conf, plan.toSocket(e.getExecutorID()), latch);
                    break;
                case boltType:
                case sinkType:
                    thread = launchBolt_SingleCore(e, new TopologyContext(g, db, plan, e, ThreadMap, HPCMonotor)
                            , conf, plan.toSocket(e.getExecutorID()), latch);
                    break;
                case virtualType:
                    LOG.info("Won't launch virtual ground");
                    if (!conf.getBoolean("NAV", true)) {
                        e.prepareProfilingStruct(conf, null, null, p);
                    }
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

        st = new spoutThread(e, context, conf, cores, node, latch, loadTargetHz, timeSliceLengthMs
                , HPCMonotor, ThreadMap, clock);

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
                HPCMonotor, optimizationManager, ThreadMap, clock);
        wt.setDaemon(true);
        if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
            wt.start();
        }
        ThreadMap.putIfAbsent(e.getExecutorID(), wt);
        return wt;
    }

    private executorThread launchSpout_SingleCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                                  int node, CountDownLatch latch) {
        spoutThread st;
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

//		LOG.info("Launch bolt:" + e.getOP() + " on node:" + node);
        long[] cpu;
        if (!conf.getBoolean("NAV", true)) {
            cpu = AC.requirePerCore(node);
        } else {
            cpu = new long[1];
        }
        return launchBolt_InCore(e, context, conf, node, cpu, latch);
    }

    private boolean migrate_complete(executorThread thread) {
        return !thread.migrating;
    }

    public void redistributeTasks(ExecutionGraph g, Configuration conf, ExecutionPlan plan)
            throws InterruptedException {
        LOG.info("BasicBoltBatchExecutor rebinding..");

        AC.clear();
//		TopologyContext[] contexts = new TopologyContext[g.getExecutionNodeArrayList().size() - 1];
//		int i = 0;
        for (executorThread thread : ThreadMap.values()) {
            int toSocket = plan.toSocket(thread.getExecutorID());
            long[] cpu = AC.require(toSocket);
            thread.migrate(cpu);
            thread.migrate(toSocket);
            while (true) {
                if (migrate_complete(thread)) {
                    break;
                }
                Thread.sleep(1000);
            }
            LOG.info("Rebind Executors " + thread.getOP() + "-" + thread.getExecutorID() + " on core: " + Arrays.toString(cpu));
            TopologyContext.plan = plan;//GetAndUpdate context.
        }

        LOG.info("At this point, all threads are re-scheduled successfully.");
        LOG.info("Migration complete");
    }

    private long[] rebinding(long[] cpu) {

        int bufSize = (g.topology.getPlatform().num_cores + 64 - 1) / 64;//because the change of affinity, num_cpu() will change..
        long[] cpuMask = new long[bufSize];
        LOG.info("Newly created:" + g.topology.getPlatform().num_cores);

        try {
            for (long i : cpu) {
                cpuMask[(int) (i / 64)] |= 1L << (i % 64); //Create a bit mask setting a partition CPU on
            }
        } catch (java.lang.ArrayIndexOutOfBoundsException e) {
            LOG.info("Problematic");
            LOG.info("EM:" + Arrays.toString(cpuMask));

        }

        try {
            setAffinity(cpuMask);
        } catch (java.lang.Exception e) {
            LOG.info("Problematic");
            LOG.info("EM:" + Arrays.toString(cpuMask));
        }
        LOG.info("Normal");
        LOG.info("EM:" + Arrays.toString(cpuMask));
        return getAffinity();
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
        if (CONTROL.enable_shared_state && tp_engine != null)
            tp_engine.engine_shutdown();
    }

    public executorThread getSinkThread() {
        return ThreadMap.get(g.getSinkThread());
    }
}
