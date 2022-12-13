package optimization;

import common.collections.Configuration;
import components.exception.UnhandledCaseException;
import db.Database;
import execution.ExecutionGraph;
import execution.ExecutionManager;
import execution.runtime.executorThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Only naive distribute is supported.
     *
     * @param db
     * @throws UnhandledCaseException
     */
    public void lanuch(Database db) throws UnhandledCaseException {
        EM = new ExecutionManager(g, conf, this);
        latch = new CountDownLatch(g.getExecutionNodeArrayList().size() + 1 - 1);//+1:OM -1:virtual
        EM.distributeTasks(conf, latch, db);
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
        //if (enable_log) LOG.info("Optimization manager exists");
    }

    @Override
    protected void _execute_noControl() {
    }

    @Override
    protected void _execute() {
    }

    @Override
    protected void _execute_with_index(int index) {
    }
}
