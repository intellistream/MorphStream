package stage;

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
import utils.SOURCE_CONTROL;

/**
 * One Stage One Scheduler + One Control.
 */
public class Stage {
    /**
     * Scheduler for static workload
     */
    private IScheduler scheduler; // transaction execution scheduler

    private SOURCE_CONTROL control = new SOURCE_CONTROL();

    public SOURCE_CONTROL getControl() {
        return control;
    }

    public IScheduler getScheduler() {
        return scheduler;
    }

    public void CreateScheduler(String schedulerType, int threadCount, int numberOfStates, int app) {
        switch (schedulerType) {
            case "OG_BFS": // Group of operation + Structured BFS exploration strategy + coarse-grained
                scheduler = new OGBFSScheduler(threadCount, numberOfStates, app);
                break;
            case "OG_BFS_A": // Group of operation + Structured BFS exploration strategy + fine-grained
                scheduler = new OGBFSAScheduler(threadCount, numberOfStates, app);
                break;
            case "OG_DFS": // Group of operation + Structured DFS exploration strategy + coarse-grained
                scheduler = new OGDFSScheduler(threadCount, numberOfStates, app);
                break;
            case "OG_DFS_A": // Group of operation + Structured DFS exploration strategy + fine-grained
                scheduler = new OGDFSAScheduler(threadCount, numberOfStates, app);
                break;
            case "OG_NS": // Group of operation + Non-structured exploration strategy + coarse-grained
                scheduler = new OGNSScheduler(threadCount, numberOfStates, app);
                break;
            case "OG_NS_A": // Group of operation + Non-structured exploration strategy + fine-grained
                scheduler = new OGNSAScheduler(threadCount, numberOfStates, app);
                break;
            case "OP_NS": // Single operation + Non-structured exploration strategy + coarse-grained
                scheduler = new OPNSScheduler<>(threadCount, numberOfStates, app);
                break;
            case "OP_NS_A": // Single operation + Non-structured exploration strategy + fine-grained
                scheduler = new OPNSAScheduler<>(threadCount, numberOfStates, app);
                break;
            case "OP_BFS": // Single operation + Structured BFS exploration strategy + coarse-grained
                scheduler = new OPBFSScheduler<>(threadCount, numberOfStates, app);
                break;
            case "OP_BFS_A": // Single operation + Structured BFS exploration strategy + fine-grained
                scheduler = new OPBFSAScheduler<>(threadCount, numberOfStates, app);
                break;
            case "OP_DFS": // Single operation + Structured DFS exploration strategy + coarse-grained
                scheduler = new OPDFSScheduler<>(threadCount, numberOfStates, app);
                break;
            case "OP_DFS_A": // Single operation + Structured DFS exploration strategy + fine-grained
                scheduler = new OPDFSAScheduler<>(threadCount, numberOfStates, app);
                break;
            case "TStream": // original TStream also uses Non-structured exploration strategy
                scheduler = new TStreamScheduler(threadCount, numberOfStates, app);
                break;
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + schedulerType);
        }
        scheduler.initTPG(0);
    }

    public void CreateController(int totalThread) {
        control.config(totalThread);
    }
}
