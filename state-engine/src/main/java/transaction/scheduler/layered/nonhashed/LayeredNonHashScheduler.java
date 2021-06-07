package transaction.scheduler.layered.nonhashed;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.IScheduler;
import transaction.scheduler.layered.LayeredScheduler;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;

public abstract class LayeredNonHashScheduler<V extends Collection<OperationChain>> extends LayeredScheduler implements IScheduler {
    NonHashContext<V> context;

    public HashMap<Integer, ArrayDeque<OperationChain>> buildTempBucketPerThread(int threadId,
                                                                                 Collection<OperationChain> ocs) {
        MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = new HashMap<>();
        int localMaxDLevel = buildBucketPerThread(layeredOCBucketThread, ocs);
        updateMaxDLevel(localMaxDLevel);
        MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
        return layeredOCBucketThread;
    }
    private void updateMaxDLevel(int localMaxDLevel) {
        synchronized (context) {
            if (context.maxDLevel < localMaxDLevel)
                context.maxDLevel = localMaxDLevel;
        }
    }
    /**
     * Build a temporary OC bucket per thread, and then merge them together to a global OC bucket.
     *
     * @param threadId
     * @param ocs
     */
    @Override
    public void SUBMIT(int threadId, Collection<OperationChain> ocs) {
        MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = buildTempBucketPerThread(threadId, ocs);
        for (var dLevel : layeredOCBucketThread.keySet())
            context.layeredOCBucketGlobal.putIfAbsent(dLevel, context.createContents());
        for (var dLevel : layeredOCBucketThread.keySet()) {
            V dLevelList = context.layeredOCBucketGlobal.get(dLevel);
            synchronized (dLevelList) {//TODO: check if this synchronize correctly.
                dLevelList.addAll(layeredOCBucketThread.get(dLevel));//merge operation chains at the same level together.
            }
        }
        MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
    }
}
