package intellistream.morphstream.engine.txn.stage;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured.OGNSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured.OGNSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured.TStreamScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.structured.OGBFSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.structured.OGBFSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.structured.OGDFSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.structured.OGDFSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.nonstructured.OPNSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.nonstructured.OPNSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.structured.OPBFSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.structured.OPBFSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.structured.OPDFSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.structured.OPDFSScheduler;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;

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

    public void CreateScheduler(String schedulerType, int threadCount, int numberOfStates) {
        switch (schedulerType) {
            case "OG_BFS": // Group of operation + Structured BFS exploration strategy + coarse-grained
                scheduler = new OGBFSScheduler(threadCount, numberOfStates);
                break;
            case "OG_BFS_A": // Group of operation + Structured BFS exploration strategy + fine-grained
                scheduler = new OGBFSAScheduler(threadCount, numberOfStates);
                break;
            case "OG_DFS": // Group of operation + Structured DFS exploration strategy + coarse-grained
                scheduler = new OGDFSScheduler(threadCount, numberOfStates);
                break;
            case "OG_DFS_A": // Group of operation + Structured DFS exploration strategy + fine-grained
                scheduler = new OGDFSAScheduler(threadCount, numberOfStates);
                break;
            case "OG_NS": // Group of operation + Non-structured exploration strategy + coarse-grained
                scheduler = new OGNSScheduler(threadCount, numberOfStates);
                break;
            case "OG_NS_A": // Group of operation + Non-structured exploration strategy + fine-grained
                scheduler = new OGNSAScheduler(threadCount, numberOfStates);
                break;
            case "OP_NS": // Single operation + Non-structured exploration strategy + coarse-grained
                scheduler = new OPNSScheduler<>(threadCount, numberOfStates);
                break;
            case "OP_NS_A": // Single operation + Non-structured exploration strategy + fine-grained
                scheduler = new OPNSAScheduler<>(threadCount, numberOfStates);
                break;
            case "OP_BFS": // Single operation + Structured BFS exploration strategy + coarse-grained
                scheduler = new OPBFSScheduler<>(threadCount, numberOfStates);
                break;
            case "OP_BFS_A": // Single operation + Structured BFS exploration strategy + fine-grained
                scheduler = new OPBFSAScheduler<>(threadCount, numberOfStates);
                break;
            case "OP_DFS": // Single operation + Structured DFS exploration strategy + coarse-grained
                scheduler = new OPDFSScheduler<>(threadCount, numberOfStates);
                break;
            case "OP_DFS_A": // Single operation + Structured DFS exploration strategy + fine-grained
                scheduler = new OPDFSAScheduler<>(threadCount, numberOfStates);
                break;
            case "TStream": // original TStream also uses Non-structured exploration strategy
                scheduler = new TStreamScheduler(threadCount, numberOfStates);
                break;
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + schedulerType);
        }
        scheduler.initTPG(0);
    }

    public void CreateController(int totalThread) {
        if (MorphStreamEnv.get().configuration().getBoolean("isGroup")) {
            control.config(totalThread, MorphStreamEnv.get().configuration().getInt("groupNum"));
        } else {
            control.config(totalThread, 1);
        }
    }
}
