package transaction.scheduler;
import java.util.concurrent.ConcurrentHashMap;
public abstract class LayeredScheduler<V> implements IScheduler {

    protected ConcurrentHashMap<Integer, V> dLevelBasedOCBuckets;
    protected int[] currentDLevelToProcess;
    public LayeredScheduler(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap(); // TODO: make sure this will not cause trouble with multithreaded access.
        currentDLevelToProcess = new int[tp];
    }
}
