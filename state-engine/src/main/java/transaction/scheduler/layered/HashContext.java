package transaction.scheduler.layered;

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
    @Override
    public boolean finished(int threadId) {
        return scheduledOcsCount[threadId] == totalOcsToSchedule[threadId];
    }

    @Override
    protected void reset() {
        super.reset();
        for (int threadId = 0; threadId < totalThreads; threadId++) {
            totalOcsToSchedule[threadId] = 0;
            scheduledOcsCount[threadId] = 0;
        }
    }
}
