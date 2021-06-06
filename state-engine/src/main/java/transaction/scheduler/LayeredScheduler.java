package transaction.scheduler;

import java.util.concurrent.ConcurrentHashMap;

public abstract class LayeredScheduler<V> extends Scheduler {
    protected ConcurrentHashMap<Integer, V> dLevelBasedOCBuckets;

    public LayeredScheduler(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap(); // TODO: make sure this will not cause trouble with multithreaded access.
        currentDLevelToProcess = new int[tp];
    }
}
