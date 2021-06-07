package transaction.scheduler.layered.hashed;

import transaction.scheduler.layered.LayeredContext;
/**
 * auxiliary data structure for hash-based schedulers.
 */
public class HashContext<V> extends LayeredContext<V> {
    int totalThread;
    public int[] scheduledOcsCount;//current number of operation chains processed per thread.
    public int[] totalOcsToSchedule;//total number of operation chains to process per thread.

    public HashContext(int tp) {
        super(tp);
        totalThread = tp;
        scheduledOcsCount = new int[tp];
        totalOcsToSchedule = new int[tp];
    }
    public void reset() {
        for (int lop = 0; lop < totalThread; lop++) {
            totalOcsToSchedule[lop] = 0;
            scheduledOcsCount[lop] = 0;
        }
    }
}
