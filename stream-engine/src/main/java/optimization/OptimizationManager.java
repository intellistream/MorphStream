package optimization;
import common.collections.Configuration;
import common.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import components.exception.UnhandledCaseException;
import execution.ExecutionGraph;
import execution.ExecutionManager;
import execution.runtime.executorThread;
import state_engine.db.Database;

import java.util.concurrent.CountDownLatch;
/**
 * Created by I309939 on 11/8/2016.
 */
public class OptimizationManager extends executorThread {
    private final static Logger LOG = LoggerFactory.getLogger(OptimizationManager.class);
    private final Configuration conf;
    public ExecutionGraph g;
    public CountDownLatch latch;
    private ExecutionManager EM;
    public OptimizationManager(ExecutionGraph g, Configuration conf) {
        super(null, conf, null, null, 0, null, null);
        this.g = g;
        this.conf = conf;

    }
    public ExecutionManager getEM() {
        return EM;
    }

    /**
     * Only naive distribute is supported in sesame.
     * @param p
     * @param db
     * @throws UnhandledCaseException
     */
    public void lanuch(Platform p, Database db) throws UnhandledCaseException {
        EM = new ExecutionManager(g, conf, this, db, p);
        //Prepared only
        latch = new CountDownLatch(g.getExecutionNodeArrayList().size() + 1 - 1);//+1:OM -1:virtual
        LOG.info("Native execution");
        EM.distributeTasks(conf, latch, db, p);
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
