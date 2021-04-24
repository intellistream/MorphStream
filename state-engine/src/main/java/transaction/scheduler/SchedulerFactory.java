package transaction.scheduler;

public class SchedulerFactory {

    public enum SCHEDULER_TYPE {
        BL,
        RR,
        SW,
        NB_BL,
        NB_RR,
        NB_SW,
        G_S,
    }

    private int totalThread;
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
            case NB_BL:
                scheduler = new NoBarrierBaseLineScheduler(totalThread);
                break;
            case NB_SW:
                scheduler = new NoBarrierSharedWorkload(totalThread);
                break;
            case G_S:
                scheduler = new GreedySmartScheduler(totalThread);
                break;
        }
        return scheduler;
    }
    
}
