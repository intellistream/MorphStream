package transaction;

import scheduler.impl.IScheduler;
import scheduler.impl.OCScheduler;
import scheduler.impl.layered.BFSScheduler;
import scheduler.impl.layered.BFSSchedulerWithAbort;
import scheduler.impl.layered.DFSScheduler;
import scheduler.impl.layered.DFSSchedulerWithAbort;
import scheduler.impl.nonlayered.GSScheduler;
import scheduler.impl.nonlayered.GSSchedulerWithAbort;
import scheduler.oplevel.context.OPGSTPGContext;
import scheduler.oplevel.impl.tpg.OPBFSScheduler;
import scheduler.oplevel.impl.tpg.OPBFSSchedulerWithAbort;
import scheduler.oplevel.impl.tpg.OPGSScheduler;
import scheduler.oplevel.impl.tpg.OPGSSchedulerWithAbort;

/**
 * Every thread has its own TxnManager.
 */
public abstract class TxnManager implements ITxnManager {
    protected static IScheduler scheduler;

    public static void CreateScheduler(String schedulerType, int threadCount, int numberOfStates) {

        switch (schedulerType) {
            case "BFS":
                scheduler = new BFSScheduler(threadCount, numberOfStates);
                break;
            case "BFSA":
                scheduler = new BFSSchedulerWithAbort(threadCount, numberOfStates);
                break;
            case "DFS": // TODO
                scheduler = new DFSScheduler(threadCount, numberOfStates);
                break;
            case "DFSA": // TODO
                scheduler = new DFSSchedulerWithAbort(threadCount, numberOfStates);
                break;
            case "GS":
                scheduler = new GSScheduler(threadCount, numberOfStates);
                break;
            case "GSA":
                scheduler = new GSSchedulerWithAbort(threadCount, numberOfStates);
                break;
            case "OPGS":
                scheduler = new OPGSScheduler<>(threadCount, numberOfStates);
                break;
            case "OPGSA":
                scheduler = new OPGSSchedulerWithAbort<>(threadCount, numberOfStates);
                break;
            case "OPBFS":
                scheduler = new OPBFSScheduler<>(threadCount, numberOfStates);
                break;
            case "OPBFSA":
                scheduler = new OPBFSSchedulerWithAbort<>(threadCount, numberOfStates);
                break;
            case "OPDFS":
                scheduler = new OPBFSScheduler<>(threadCount, numberOfStates);
                break;
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + schedulerType);
        }
    }
}
