package transaction.scheduler.layered;

import index.high_scale_lib.ConcurrentHashMap;
import transaction.scheduler.SchedulerContext;

import java.util.function.Supplier;
public class LayeredContext<V> extends SchedulerContext {

    public int[] currentLevel;
    protected int[] scheduledOcsCount;//current number of operation chains processed per thread.
    protected int[] totalOcsToSchedule;//total number of operation chains to process per thread.

    protected int[] totalOsToSchedule;//total number of operations to process per thread.

    public ConcurrentHashMap<Integer, V> layeredOCBucketGlobal;
    protected Supplier<V> supplier;
    public int totalThreads;

    public LayeredContext(int totalThreads, Supplier<V> supplier) {
        layeredOCBucketGlobal = new ConcurrentHashMap<>();
        currentLevel = new int[totalThreads];
        this.supplier = supplier;
        scheduledOcsCount = new int[totalThreads];
        totalOcsToSchedule = new int[totalThreads];

        totalOsToSchedule = new int[totalThreads];
    }
    public V createContents() {
        return supplier.get();
    }

    @Override
    protected void reset() {
        layeredOCBucketGlobal.clear();
        for (int threadId = 0; threadId < totalThreads; threadId++) {
            currentLevel[threadId] = 0;
        }
        for (int threadId = 0; threadId < totalThreads; threadId++) {
            totalOcsToSchedule[threadId] = 0;
            scheduledOcsCount[threadId] = 0;
        }
    }
    @Override
    protected boolean finished(int threadId) {
        return scheduledOcsCount[threadId] == totalOcsToSchedule[threadId];
    }
}

