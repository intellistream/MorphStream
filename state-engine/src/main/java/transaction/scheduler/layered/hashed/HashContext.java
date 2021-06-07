package transaction.scheduler.layered.hashed;

import transaction.scheduler.layered.LayeredContext;

import java.util.function.Supplier;
/**
 * auxiliary data structure for hash-based schedulers.
 */
public class HashContext<V> extends LayeredContext<V> {
    protected int[] scheduledOcsCount;//current number of operation chains processed per thread.
    protected int[] totalOcsToSchedule;//total number of operation chains to process per thread.

    public HashContext(int totalThread, Supplier<V> supplier) {
        super(totalThread, supplier);
        scheduledOcsCount = new int[totalThread];
        totalOcsToSchedule = new int[totalThread];
    }
    public void reset() {
        super.reset();
        for (int threadId = 0; threadId < totalThreads; threadId++) {
            totalOcsToSchedule[threadId] = 0;
            scheduledOcsCount[threadId] = 0;
        }
    }
}
