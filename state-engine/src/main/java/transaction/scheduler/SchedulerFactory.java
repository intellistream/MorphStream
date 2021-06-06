package transaction.scheduler;


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
            case LRR:
                scheduler= new Scheduler1( totalThread);
                break;
        }
        return scheduler;
    }

    public enum SCHEDULER_TYPE {
        LRR,//Layered-Random-Roundrobin.
    }

}
