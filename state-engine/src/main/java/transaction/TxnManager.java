package transaction;

import scheduler.impl.IScheduler;
import scheduler.impl.OCScheduler;
import scheduler.impl.layered.BFSScheduler;
import scheduler.impl.layered.BFSSchedulerWithAbort;
import scheduler.impl.layered.DFSScheduler;
import scheduler.impl.layered.DFSSchedulerWithAbort;
import scheduler.impl.nonlayered.GSScheduler;
import scheduler.impl.nonlayered.GSSchedulerWithAbort;
import scheduler.impl.nonlayered.TStreamScheduler;
import scheduler.oplevel.context.OPGSTPGContext;
import scheduler.oplevel.impl.tpg.*;
import utils.UDF;

/**
 * Every thread has its own TxnManager.
 */
public abstract class TxnManager implements ITxnManager {
    protected static IScheduler scheduler;

    public static void CreateScheduler(String schedulerType, int threadCount, int numberOfStates, int app) {
        switch (schedulerType) {
            case "BFS":
                scheduler = new BFSScheduler(threadCount, numberOfStates, app);
                break;
            case "BFSA":
                scheduler = new BFSSchedulerWithAbort(threadCount, numberOfStates, app);
                break;
            case "DFS": // TODO
                scheduler = new DFSScheduler(threadCount, numberOfStates, app);
                break;
            case "DFSA": // TODO
                scheduler = new DFSSchedulerWithAbort(threadCount, numberOfStates, app);
                break;
            case "GS":
                scheduler = new GSScheduler(threadCount, numberOfStates, app);
                break;
            case "TStream": // original tstream also uses gs scheduler
                scheduler = new TStreamScheduler(threadCount, numberOfStates, app);
                break;
            case "GSA":
                scheduler = new GSSchedulerWithAbort(threadCount, numberOfStates, app);
                break;
            case "OPGS":
                scheduler = new OPGSScheduler<>(threadCount, numberOfStates, app);
                break;
            case "OPGSA":
                scheduler = new OPGSSchedulerWithAbort<>(threadCount, numberOfStates, app);
                break;
            case "OPBFS":
                scheduler = new OPBFSScheduler<>(threadCount, numberOfStates, app);
                break;
            case "OPBFSA":
                scheduler = new OPBFSSchedulerWithAbort<>(threadCount, numberOfStates, app);
                break;
            case "OPDFS":
                scheduler = new OPDFSScheduler<>(threadCount, numberOfStates, app);
                break;
            case "OPDFSA":
                scheduler = new OPDFSSchedulerWithAbort<>(threadCount, numberOfStates, app);
                break;
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + schedulerType);
        }
    }
}
