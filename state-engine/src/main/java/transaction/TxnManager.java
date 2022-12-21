package transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.impl.IScheduler;
import scheduler.impl.og.nonstructured.OGNSAScheduler;
import scheduler.impl.og.nonstructured.OGNSScheduler;
import scheduler.impl.og.nonstructured.TStreamScheduler;
import scheduler.impl.og.structured.OGBFSAScheduler;
import scheduler.impl.og.structured.OGBFSScheduler;
import scheduler.impl.og.structured.OGDFSAScheduler;
import scheduler.impl.og.structured.OGDFSScheduler;
import scheduler.impl.op.nonstructured.OPNSAScheduler;
import scheduler.impl.op.nonstructured.OPNSScheduler;
import scheduler.impl.op.structured.OPBFSAScheduler;
import scheduler.impl.op.structured.OPBFSScheduler;
import scheduler.impl.op.structured.OPDFSAScheduler;
import scheduler.impl.op.structured.OPDFSScheduler;
import stage.Stage;


/**
 * Every thread has its own TxnManager.
 */
public abstract class TxnManager implements ITxnManager {
    private static final Logger log = LoggerFactory.getLogger(TxnManager.class);
    public final Stage stage;

    protected TxnManager(Stage stage) {
        this.stage = stage;
    }

    /**
     * create Scheduler by flag
     *
     * @param schedulerType
     * @param threadCount
     * @param numberOfStates
     * @param app
     * @return
     */
    public static IScheduler CreateSchedulerByType(String schedulerType, int threadCount, int numberOfStates, int app) {
        switch (schedulerType) {
            case "OG_BFS": // Group of operation + Structured BFS exploration strategy + coarse-grained
                return new OGBFSScheduler(threadCount, numberOfStates, app);
            case "OG_BFS_A": // Group of operation + Structured BFS exploration strategy + fine-grained
                return new OGBFSAScheduler(threadCount, numberOfStates, app);
            case "OG_DFS": // Group of operation + Structured DFS exploration strategy + coarse-grained
                return new OGDFSScheduler(threadCount, numberOfStates, app);
            case "OG_DFS_A": // Group of operation + Structured DFS exploration strategy + fine-grained
                return new OGDFSAScheduler(threadCount, numberOfStates, app);
            case "OG_NS": // Group of operation + Non-structured exploration strategy + coarse-grained
                return new OGNSScheduler(threadCount, numberOfStates, app);
            case "OG_NS_A": // Group of operation + Non-structured exploration strategy + fine-grained
                return new OGNSAScheduler(threadCount, numberOfStates, app);
            case "OP_NS": // Single operation + Non-structured exploration strategy + coarse-grained
                return new OPNSScheduler<>(threadCount, numberOfStates, app);
            case "OP_NS_A": // Single operation + Non-structured exploration strategy + fine-grained
                return new OPNSAScheduler<>(threadCount, numberOfStates, app);
            case "OP_BFS": // Single operation + Structured BFS exploration strategy + coarse-grained
                return new OPBFSScheduler<>(threadCount, numberOfStates, app);
            case "OP_BFS_A": // Single operation + Structured BFS exploration strategy + fine-grained
                return new OPBFSAScheduler<>(threadCount, numberOfStates, app);
            case "OP_DFS": // Single operation + Structured DFS exploration strategy + coarse-grained
                return new OPDFSScheduler<>(threadCount, numberOfStates, app);
            case "OP_DFS_A": // Single operation + Structured DFS exploration strategy + fine-grained
                return new OPDFSAScheduler<>(threadCount, numberOfStates, app);
            case "TStream": // original TStream also uses Non-structured exploration strategy
                return new TStreamScheduler(threadCount, numberOfStates, app);
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + schedulerType);
        }
    }

}
