package transaction.scheduler;

import transaction.scheduler.layered.hashed.DFSLayeredHashScheduler;
import transaction.scheduler.layered.hashed.LayeredHashScheduler;
import transaction.scheduler.layered.nonhashed.DFSLayeredRoundRobinScheduler;
import transaction.scheduler.layered.nonhashed.DFSLayeredSharedWorkloadScheduler;
import transaction.scheduler.layered.nonhashed.LayeredRoundRobinScheduler;
import transaction.scheduler.layered.nonhashed.LayeredSharedWorkloadScheduler;

/**
 * Author: Aqif Hamid
 * A single point schedulers creation factory.
 */
public class SchedulerFactory {

    private final int totalThread;

    public SchedulerFactory(int tp) {
        totalThread = tp;
    }
    public IScheduler CreateScheduler(SCHEDULER_TYPE schedulerType) {

        IScheduler scheduler = null;
        switch (schedulerType) {

            case BL:
                scheduler = new LayeredHashScheduler(totalThread);
                break;
            case RR:
                scheduler = new LayeredRoundRobinScheduler(totalThread);
                break;
            case SW:
                scheduler = new LayeredSharedWorkloadScheduler(totalThread);
                break;
            case NB_RR:
                scheduler = new DFSLayeredRoundRobinScheduler(totalThread);
                break;
            case NB_BL:
                scheduler = new DFSLayeredHashScheduler(totalThread);
                break;
            case NB_SW:
                scheduler = new DFSLayeredSharedWorkloadScheduler(totalThread);
                break;
            case G_S:
                scheduler = new GreedySmartScheduler(totalThread);
                break;
        }
        return scheduler;
    }

    public enum SCHEDULER_TYPE {
        BL,
        RR,
        SW,
        NB_BL,
        NB_RR,
        NB_SW,
        G_S,
    }

}
