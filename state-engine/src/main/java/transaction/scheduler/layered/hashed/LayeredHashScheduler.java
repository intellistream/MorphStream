package transaction.scheduler.layered.hashed;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.IScheduler;
import transaction.scheduler.layered.LayeredScheduler;
import utils.SOURCE_CONTROL;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;

public class LayeredHashScheduler extends LayeredScheduler<HashMap<Integer, ArrayDeque<OperationChain>>> implements IScheduler {

    protected int[] scheduledOcsCount;//current number of operation chains processed per thread.
    protected int[] totalOcsToSchedule;//total number of operation chains to process per thread.

    public LayeredHashScheduler(int tp) {
        super(tp);
        scheduledOcsCount = new int[tp];
        totalOcsToSchedule = new int[tp];

        for (int threadId = 0; threadId < tp; threadId++) {
            layeredOCBucketGlobal.put(threadId, new HashMap<>());
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
        totalOcsToSchedule[threadId] += ocs.size();
        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = layeredOCBucketGlobal.get(threadId);
        buildBucketPerThread(layeredOCBucketThread, ocs);
        MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
    }

    /**
     * Process all operation chains of one layer until another layer.
     * It's more like a breath-first-search.
     *
     * @param threadId
     * @return
     */
    @Override
    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = super.nextOperationChain(threadId);
        if (oc != null)
            scheduledOcsCount[threadId] += 1;

        if (finishedScheduling(threadId)) {//finished scheduling already. This is needed because hash-scheduler can be workload unbalanced.
            MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
            SOURCE_CONTROL.getInstance().oneThreadCompleted();
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
        }

        return oc; // if a null is returned, it means, we are done with level!
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param threadId
     * @param dLevel
     * @return
     */
    protected OperationChain getOC(int threadId, int dLevel) {
        ArrayDeque<OperationChain> ocs = layeredOCBucketGlobal.get(threadId).get(dLevel);
        if (ocs.size() > 0)
            return ocs.removeLast();
        else return null;
    }

    @Override
    public boolean finishedScheduling(int threadId) {
        return scheduledOcsCount[threadId] == totalOcsToSchedule[threadId];
    }

    @Override
    public void reset() {
        for (int lop = 0; lop < totalOcsToSchedule.length; lop++) {
            totalOcsToSchedule[lop] = 0;
            scheduledOcsCount[lop] = 0;
            currentLevel[lop] = 0;
        }
    }

}
