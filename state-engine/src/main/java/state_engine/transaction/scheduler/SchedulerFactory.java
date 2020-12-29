package state_engine.transaction.scheduler;

public class SchedulerFactory {

    public enum SCHEDULER_TYPE {
        BL,
        RR,
        SW,
        NB_RR,
        NB_SW,
        S_RR,
        S_SW,
        S_NB_RR,
        S_NB_SW
    }

    private int totalThread = 0;
    public SchedulerFactory(int tp) {
        totalThread = tp;
    }

    public IScheduler CreateScheduler(SCHEDULER_TYPE schedulerType) {

        IScheduler scheduler = null;
        switch(schedulerType) {

            case BL:
                scheduler = new BaseLineScheduler(totalThread);
                break;
            case RR:
                scheduler = new RoundRobinScheduler(totalThread);
                break;
            case SW:
                scheduler = new SharedWorkloadScheduler(totalThread);
                break;
            case NB_RR:
                scheduler = new NoBarrierRoundRobinScheduler(totalThread);
                break;
            case NB_SW:
                scheduler = new NoBarrierSharedWorkload(totalThread);
                break;
//            case S_RR:
//                scheduler = new SmartRRSchedulerv1(totalThread);
//                break;
//            case S_SW:
//                scheduler = new SmartSWSchedulerv1(totalThread);
//                break;
            case S_NB_RR:
                scheduler = new SmartNoBarrierRRSchedulerv1(totalThread);
                break;
            case S_NB_SW:
                scheduler = new SmartNoBarrierSWSchedulerv2(totalThread);
                break;
        }
        return scheduler;
    }
    
}
