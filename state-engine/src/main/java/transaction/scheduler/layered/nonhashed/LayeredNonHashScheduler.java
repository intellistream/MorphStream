package transaction.scheduler.layered.nonhashed;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.IScheduler;
import transaction.scheduler.layered.LayeredScheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public abstract class LayeredNonHashScheduler<V> extends LayeredScheduler<V> implements IScheduler {
    protected Integer maxDLevel;
    protected int totalThreads;

    public LayeredNonHashScheduler(int totalThreads) {
        super(totalThreads);
        this.totalThreads = totalThreads;
        maxDLevel = 0;
    }

    public HashMap<Integer, List<OperationChain>> buildTempBucketPerThread(int threadId,
                                                                           Collection<OperationChain> ocs) {
        MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
        HashMap<Integer, List<OperationChain>> layeredOCBucketThread = new HashMap<>();
        int localMaxDLevel = buildBucketPerThread(layeredOCBucketThread, ocs);

        synchronized (maxDLevel) {
            MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
            if (maxDLevel < localMaxDLevel)
                maxDLevel = localMaxDLevel;
        }
        return layeredOCBucketThread;
    }
}
