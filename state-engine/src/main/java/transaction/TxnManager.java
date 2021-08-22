package transaction;

import scheduler.impl.IScheduler;
import scheduler.impl.Scheduler;
import scheduler.impl.layered.BFSScheduler;
import scheduler.impl.layered.DFSScheduler;
import scheduler.impl.nonlayered.GSScheduler;

/**
 * Every thread has its own TxnManager.
 */
public abstract class TxnManager implements ITxnManager {
    protected static Scheduler scheduler;

    public static void CreateScheduler(String schedulerType, int threadCount, int numberOfStates) {

        switch (schedulerType) {
            case "BFS":
                scheduler = new BFSScheduler(threadCount, numberOfStates);
                break;
            case "DFS": // TODO
                scheduler = new DFSScheduler(threadCount, numberOfStates);
                break;
            case "GS":
                scheduler = new GSScheduler(threadCount, numberOfStates);
                break;
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + schedulerType);
        }
    }
}
