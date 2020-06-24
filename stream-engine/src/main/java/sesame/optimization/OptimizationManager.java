package sesame.optimization;
import common.platform.Platform;
import common.collections.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.exception.UnhandledCaseException;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionManager;
import sesame.execution.runtime.executorThread;
import state_engine.Database;

import java.util.concurrent.CountDownLatch;
/**
 * Created by I309939 on 11/8/2016.
 */
public class OptimizationManager extends executorThread {
    private final static Logger LOG = LoggerFactory.getLogger(OptimizationManager.class);
    private final Configuration conf;
    public int end_executor = 1;
    public ExecutionGraph g;
    public CountDownLatch latch;
    private ExecutionPlan executionPlan;
    private ExecutionManager EM;
    private long profiling_gaps = 10000;//10 seconds.
    public OptimizationManager(ExecutionGraph g, Configuration conf) {
        super(null, conf, null, null, 0, null, null, null);
        this.g = g;
        this.conf = conf;
    }
    public ExecutionManager getEM() {
        return EM;
    }
    private void profile_eachThread() {
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
    public ExecutionPlan lanuch(Platform p, Database db) {
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
    /**
     * creates new txn and routing plan continuously.
     */
    public void run() {
        this.node = 0;
        latch.countDown();          //tells others I'm ready.
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //TODO: add optimization module here.

        LOG.info("Optimization manager exists");
    }
    @Override
    protected void _execute_noControl() {
    }
    @Override
    protected void _execute() {
    }
}
