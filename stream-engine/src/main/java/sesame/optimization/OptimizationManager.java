package sesame.optimization;

import application.Platform;
import application.util.Configuration;
import application.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.Topology;
import sesame.components.TopologyComponent;
import sesame.components.exception.UnhandledCaseException;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionManager;
import sesame.execution.ExecutionNode;
import sesame.execution.runtime.executorThread;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.routing.RoutingOptimizer;
import sesame.optimization.routing.RoutingPlan;
import state_engine.Database;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import static application.Constants.MAP_Path;

/**
 * Created by I309939 on 11/8/2016.
 */
public class OptimizationManager extends executorThread {
    private final static Logger LOG = LoggerFactory.getLogger(OptimizationManager.class);
    private final Configuration conf;
    private final int end_cnt = 50;//50* 10=500 seconds per executor maximally
    private final long warmup_gaps = (long) (60 * 1E3);//60 seconds.
    private final boolean profile;
    private final RoutingOptimizer ro;
    private final String prefix;
    public int start_executor = 0;
    public int end_executor = 1;
    public ExecutionGraph g;
    public CountDownLatch latch;
    private Optimizer so;
    private ExecutionPlan executionPlan;
    private ExecutionManager EM;
    private long profiling_gaps = 10000;//10 seconds.
    private int profile_start = 0;
    private int profile_end = 1;
    private Topology topology;

    public OptimizationManager(ExecutionGraph g, Configuration conf, boolean profile, double relax, Platform p) {
        super(null, conf, null, null, 0, null, null, null);
        this.g = g;
        this.conf = conf;
        this.profile = profile;
        boolean benchmark = conf.getBoolean("benchmark", false);
        prefix = conf.getConfigPrefix();
        so = new Optimizer(g, conf.getBoolean("benchmark", false), conf, p, null);
        ro = new RoutingOptimizer(g);
    }

    public ExecutionManager getEM() {
        return EM;
    }

    private void profile_eachThread() {
        int cnt;
        executionPlan.profile_executor = 0;//start_executor reverse profile...
        end_executor = g.getSink().getExecutorID();// Math.min(end_executor, g.getExecutionNodeArrayList().size());//profile all threads except virtual.
        try {
            while (executionPlan.profile_executor <= end_executor) {
                executorThread thread = EM.ThreadMap.get(executionPlan.profile_executor);
                if (thread == null) {
                    LOG.info("Get a null thread at" + executionPlan.profile_executor);
                } else if (thread.executor.needsProfile()) {
                    profiling_gaps = profiling_gaps * Math.max(1, thread.executor.getParents_keySet().size());

                    thread.profiling = true;
                    while (thread.profiling) {
                        //LOG.DEBUG("Wait for profiling gaps" + profiling_gaps + "...for executor:" + executionPlan.profile_executor);
                        sleep(profiling_gaps);
                    }
                }
                //LOG.DEBUG(executionPlan.profile_executor + " finished profile, go for the next");
                executionPlan.profile_executor++;

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void read_meta() throws FileNotFoundException {
        String path = MAP_Path + OsUtils.OS_wrapper(prefix);
        File meta = new File(path.concat(OsUtils.OS_wrapper("Meta")));
        if (meta.exists()) {
            Scanner sc = new Scanner(meta);
            String[] split = sc.nextLine().split(" ");
            profile_start = Integer.parseInt(split[1]);

            split = sc.nextLine().split(" ");
            profile_end = Integer.parseInt(split[1]);


            split = sc.nextLine().split(" ");
            start_executor = Integer.parseInt(split[1]);

            split = sc.nextLine().split(" ");
            end_executor = Integer.parseInt(split[1]);

            //overwrite by command line:
            int profile_start_fromCMD = conf.getInt("profile_start", -1);
            if (profile_start_fromCMD != -1) {
                this.profile_start = profile_start_fromCMD;
            }

            int profile_end_fromCMD = conf.getInt("profile_end", -1);
            if (profile_end_fromCMD != -1) {
                this.profile_end = profile_end_fromCMD;
            }

        } else {

        }
    }

    public ExecutionPlan lanuch(Topology topology, Platform p, Database db) {
        this.topology = topology;
        EM = new ExecutionManager(g, conf, this, db, p);
        //Prepared only
        latch = new CountDownLatch(g.getExecutionNodeArrayList().size() + 1 - 1);//+1:OM -1:virtual
        try {
            LOG.info("Native execution");
            executionPlan = new ExecutionPlan(null, null);
            executionPlan.setProfile();
            EM.distributeTasks(conf, executionPlan, latch, db, p);
        } catch (UnhandledCaseException e) {
            e.printStackTrace();
        }
        return executionPlan;
    }

    private void storeSTAT() {
        for (ExecutionNode executor : g.getExecutionNodeArrayList()) {
            if (executor.needsProfile()) {
                if (executor.isSourceNode()) {
                    executor.profiling.get(-1).finishProfile();
                } else {
                    for (TopologyComponent parent : executor.getParents_keySet()) {
                        ExecutionNode src = parent.getExecutorList().get((parent.getNumTasks() - 1) / 2);//use middle one as producer..
                        executor.profiling.get(src.getExecutorID()).finishProfile();
                    }
                }
            }
        }
    }

    /**
     * DUMP:
     * <p>
     * tuple fieldSize; window fieldSize; JVM fieldSize;
     * profiling results of each executor.
     * this.tuple_size
     * +   "\t" + this.operation_cycles[0]
     * +   "\t" + this.operation_cycles[1]
     * +   "\t" + this.LLC_MISS_PS[0]
     * +   "\t" + this.LLC_MISS_PS[1]
     * +   "\t" + this.LLC_REF_PS[0]
     * +   "\t" + this.LLC_REF_PS[1];
     */
    private void storeProfileInfo() {
        int tuple_size = conf.getInt("size_tuple", 0);
        int window = conf.getInt("window", 0);
        int JVM_size = conf.getInt("JVM", 0);
        File file = new File(conf.getString("metrics.output").concat(OsUtils.OS_wrapper("DUMP.txt")));
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file, true), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
//            sb.append(tuple_size).append("\t").append(window).append("\t").append(JVM_size).append("\t");
            for (ExecutionNode executionNode : g.getExecutionNodeArrayList()) {
                if (executionNode.needsProfile()) {
                    if (executionNode.isSourceNode()) {
                        sb.append("\n").append(executionNode.getOP()).append(" on src:")
                                .append("External")
                                .append("\n").append(executionNode.profiling.get(-1).toString()).append("\n");
                    } else {
                        for (TopologyComponent parent : executionNode.getParents_keySet()) {
                            ExecutionNode srcNode = parent.getExecutorList().get((parent.getNumTasks() - 1) / 2);//use middle one as producer..
                            sb.append("\n").append(executionNode.getOP()).append(" on src:")
                                    .append(srcNode.getOP())
                                    .append("\n").append(executionNode.profiling.get(srcNode.getExecutorID()).toString()).append("\n");
                        }
                    }
                }
            }
            LOG.info("=====DUMP information=======");
            LOG.info(sb.toString());
            LOG.info("=====DUMP information=======");
            writer.write(sb.toString() + "\n");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void dumpStatistics() {
        storeSTAT();
        storeProfileInfo();
        LOG.info("Dump profiling statistics finished.");

    }


    private void warmup() {
        LOG.info("Start warmup...");
        try {
            sleep(warmup_gaps);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Warmup finished.");
    }

    private SchedulingPlan load_next_plan_toProfile() {
        SchedulingPlan schedulingPlan = so.manual_plan(profile_start++, prefix);
        executionPlan = new ExecutionPlan(schedulingPlan, null);
        executionPlan.setProfile();
        return schedulingPlan;
    }

    private void run_profile() {
        while (profile_start <= profile_end) {
            load_next_plan_toProfile();
            try {
                EM.redistributeTasks(g, conf, executionPlan);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            profile_eachThread();
        }
    }

    private void dynamic_optimize() {

        boolean parallelism_tune = conf.getBoolean("parallelism_tune", false);
        while (true) {
            try {
//                    LOG.info("Wait for optimization gaps...");
                sleep(profiling_gaps);

                SchedulingPlan schedulingPlan = so.optimize_plan();
                RoutingPlan routingPlan = ro.optimize(new RoutingPlan(g, schedulingPlan));
                executionPlan = new ExecutionPlan(schedulingPlan, routingPlan);

                EM.redistributeTasks(g, conf, executionPlan);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * creates new txn and routing plan continuously.
     */
    public void run() {

        this.node = 0;


        //use this for NUMA-awareness!
        //Disabled normally.
//        cpu = EM.AC.requirePerCore(node);
//        binding();


//        SequentialBindingInitilize();
//        LOG.info("DB initialize starts @" + DateTime.now());
//        long start = System.nanoTime();
//        g.topology.spinlock = topology.txnTopology.initializeDB();
//        long end = System.nanoTime();
//
//        LOG.info("DB initialize takes:" + (end - start) / 1E6 + " ms");

//        AffinityLock.reset();

        latch.countDown();          //tells others I'm ready.
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //initialize queue set.

//        if (conf.getBoolean("Fault_tolerance", false) || enable_shared_state) {
//            ExecutionManager.clock.start();
//        }


        if (profile) {
//            LOG.info("Wait for warm up phase...");
            warmup();
            LOG.info("Start to profile each thread...");
            profile_eachThread();
            //run_profile();//useful if we want to profile for more mapping cases..

            LOG.info("finished profiling.");
            executionPlan.disable_profile();
            EM.exist();//stop all execution threads.

            dumpStatistics();
            LOG.info("STAT ended, optimization manager exist");


        } else {
            if (conf.getBoolean("DO", false)) {
                dynamic_optimize();
            }
        }

        LOG.info("Optimization manager exists");
    }


    @Override
    protected void _execute_noControl() {

    }

    @Override
    protected void _execute() {

    }

}
