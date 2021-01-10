package state_engine.transaction.scheduler;

public class SchedulerFactory {

    public enum SCHEDULER_TYPE {
        BL,
        RR,
        RR_v2,
        SW,
        NB_BL,
        NB_RR,
        NB_RR_v2,
        NB_SW,
        S_RR,
        S_SW,
        S_NB_RR,
        S_NB_SW_v1,
        S_NB_SW_v2,
        S_NB_SW_v3,
        S_NB_SW_v4,
        S_NB_SW_v5,
        S_NB_SW_v6,
        S_NB_SW_v7,
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
            case RR_v2:
                scheduler = new RoundRobinSchedulerv2(totalThread);
                break;
            case NB_RR_v2:
                scheduler = new NoBarrierRoundRobinSchedulerv2(totalThread);
                break;
            case RR:
                scheduler = new RoundRobinScheduler(totalThread);
                break;
            case NB_RR:
                scheduler = new NoBarrierRoundRobinScheduler(totalThread);
                break;
            case SW:
                scheduler = new SharedWorkloadScheduler(totalThread);
                break;
            case NB_BL:
                scheduler = new NoBarrierBaseLineScheduler(totalThread);
                break;
            case NB_SW:
                scheduler = new NoBarrierSharedWorkload(totalThread);
                break;
            case S_NB_RR:
                scheduler = new SmartNoBarrierRRSchedulerv2(totalThread);
                break;
            case S_NB_SW_v2:
                scheduler = new SmartNoBarrierSWSchedulerv2(totalThread);
                break;
            case S_NB_SW_v3:
                scheduler = new SmartNoBarrierSWSchedulerv3(totalThread);
                break;
            case S_NB_SW_v4:
                scheduler = new SmartNoBarrierSWSchedulerv4(totalThread);
                break;
            case S_NB_SW_v5:
                scheduler = new SmartNoBarrierSWSchedulerv5(totalThread);
                break;
            case S_NB_SW_v6:
                scheduler = new SmartNoBarrierSWSchedulerv6(totalThread);
                break;
            case S_NB_SW_v7:
                scheduler = new SmartNoBarrierSWSchedulerv7(totalThread);
                break;
        }
        return scheduler;
    }
    
}
