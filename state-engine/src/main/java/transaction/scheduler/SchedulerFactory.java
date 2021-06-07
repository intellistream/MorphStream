package transaction.scheduler;

import transaction.scheduler.layered.hashed.DFSLayeredHashScheduler;
import transaction.scheduler.layered.hashed.BFSLayeredHashScheduler;
import transaction.scheduler.layered.nonhashed.rr.DFSLayeredRoundRobinScheduler;
import transaction.scheduler.layered.nonhashed.sw.DFSLayeredSharedWorkloadScheduler;
import transaction.scheduler.layered.nonhashed.rr.BFSLayeredRoundRobinScheduler;
import transaction.scheduler.layered.nonhashed.sw.BFSLayeredSharedWorkloadScheduler;
import transaction.scheduler.nonlayered.NonLayeredScheduler;

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
                scheduler = new BFSLayeredHashScheduler(totalThread);
                break;
            case RR:
                scheduler = new BFSLayeredRoundRobinScheduler(totalThread);
                break;
            case SW:
                scheduler = new BFSLayeredSharedWorkloadScheduler(totalThread);
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
                scheduler = new NonLayeredScheduler(totalThread);
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
