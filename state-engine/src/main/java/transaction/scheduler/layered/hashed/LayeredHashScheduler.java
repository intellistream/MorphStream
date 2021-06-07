package transaction.scheduler.layered.hashed;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.IScheduler;
import transaction.scheduler.layered.LayeredScheduler;
import utils.SOURCE_CONTROL;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;

public abstract class LayeredHashScheduler extends LayeredScheduler implements IScheduler {
    public HashContext<HashMap<Integer, ArrayDeque<OperationChain>>> context;
    public LayeredHashScheduler(int tp) {
        context = new HashContext<>(tp, HashMap::new);
        for (int threadId = 0; threadId < tp; threadId++) {
            context.layeredOCBucketGlobal.put(threadId, context.createContents());
        }
    }

    /**
     * Because each thread holds hash-partitioned OCs, hash partition is skipped here.
     * We construct a layeredOCBucket for each thread.
     * This method is thread safe.
     *
     * @param threadId
     * @param ocs
     */
    @Override
    public void submitOperationChains(int threadId, Collection<OperationChain> ocs) {
        MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
        context.totalOcsToSchedule[threadId] += ocs.size();
        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = context.layeredOCBucketGlobal.get(threadId);
        buildBucketPerThread(layeredOCBucketThread, ocs);
        MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param threadId
     * @param dLevel
     * @return
     */
    public OperationChain getOC(int threadId, int dLevel) {
        ArrayDeque<OperationChain> ocs = context.layeredOCBucketGlobal.get(threadId).get(dLevel);
        if (ocs.size() > 0)
            return ocs.removeLast();
        else return null;
    }

    @Override
    public boolean finishedScheduling(int threadId) {
        return context.scheduledOcsCount[threadId] == context.totalOcsToSchedule[threadId];
    }


    /**
     * This is needed because hash-scheduler can be workload unbalanced.
     *
     * @param threadId
     */
    protected void checkFinished(int threadId) {
        if (finishedScheduling(threadId)) {
            MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
            SOURCE_CONTROL.getInstance().oneThreadCompleted();
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
        }
    }
}