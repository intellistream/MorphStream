package transaction.scheduler;

import transaction.scheduler.layered.BFSLayeredHashScheduler;
import transaction.scheduler.layered.LayeredContext;
import transaction.scheduler.tpg.TPGContext;
import transaction.scheduler.tpg.TPGScheduler;

/**
 * Author: Aqif Hamid
 * A single point schedulers creation factory.
 */
public class SchedulerFactory {

    private final int totalThread;
    private final int NUM_ITEMS;

    public SchedulerFactory(int tp, int NUM_ITEMS) {
        totalThread = tp;
        this.NUM_ITEMS = NUM_ITEMS;
    }

    public IScheduler CreateScheduler(SCHEDULER_TYPE schedulerType) {

        IScheduler scheduler = null;
        switch (schedulerType) {
            case BFS:
                scheduler = new BFSLayeredHashScheduler<LayeredContext>(totalThread, NUM_ITEMS);
                break;
            case TPG:
                scheduler = new TPGScheduler<TPGContext>(totalThread);
                break;
        }
        return scheduler;
    }

    public enum SCHEDULER_TYPE {
        BFS,
        TPG
    }

}
