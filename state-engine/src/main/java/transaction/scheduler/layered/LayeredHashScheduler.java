package transaction.scheduler.layered;

import transaction.scheduler.layered.struct.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.IScheduler;
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
    public void SUBMIT(int threadId, Collection<OperationChain> ocs) {
        context.totalOcsToSchedule[threadId] += ocs.size();
        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = context.layeredOCBucketGlobal.get(threadId);
        buildBucketPerThread(layeredOCBucketThread, ocs);
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param threadId
     * @return
     */
    protected OperationChain Distribute(int threadId) {
        ArrayDeque<OperationChain> ocs = context.layeredOCBucketGlobal.get(threadId).get(context.currentLevel[threadId]);
        if (ocs.size() > 0)
            return ocs.removeLast();
        else return null;
    }

    @Override
    public boolean Finished(int threadId) {
        return context.finished(threadId);
    }

    /**
     * This is needed because hash-scheduler can be workload unbalanced.
     *
     * @param threadId
     */
    protected void checkFinished(int threadId) {
        if (Finished(threadId)) {
            SOURCE_CONTROL.getInstance().oneThreadCompleted();
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
        }
    }
}