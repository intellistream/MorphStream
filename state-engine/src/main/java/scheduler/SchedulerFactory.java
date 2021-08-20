package scheduler;

import scheduler.context.LayeredTPGContext;
import scheduler.impl.IScheduler;
import scheduler.impl.layered.BFSScheduler;
import scheduler.impl.layered.DFSScheduler;

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
                scheduler = new BFSScheduler<LayeredTPGContext>(totalThread, NUM_ITEMS);
                break;
            case DFS: // TODO: add GS
                scheduler = new DFSScheduler<LayeredTPGContext>(totalThread, NUM_ITEMS);
                break;
        }
        return scheduler;
    }

    public enum SCHEDULER_TYPE {
        BFS,
        DFS,
        GS
    }

}
