package transaction.scheduler.layered.hashed;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.IScheduler;
import transaction.scheduler.layered.LayeredScheduler;
import utils.SOURCE_CONTROL;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class LayeredHashScheduler extends LayeredScheduler<HashMap<Integer, List<OperationChain>>> implements IScheduler {

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
        HashMap<Integer, List<OperationChain>> layeredOCBucketThread = layeredOCBucketGlobal.get(threadId);
        buildBucketPerThread(layeredOCBucketThread, ocs);
        MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
    }

    /**
     * process all operation chains of one layer until another layer.
     * It's more like a breath-first-search.
     *
     * @param threadId
     * @return
     */
    @Override
    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = getOC(threadId, currentLevel[threadId]);

        if (oc != null) {
            scheduledOcsCount[threadId] += 1;
        } else if (!finishedScheduling(threadId)) {
            while (oc == null) {
                currentLevel[threadId] += 1;
                oc = getOC(threadId, currentLevel[threadId]);
                MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                SOURCE_CONTROL.getInstance().updateThreadBarrierOnDLevel(currentLevel[threadId]);
                MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
            }
            scheduledOcsCount[threadId] += 1;
        } else {
            MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
            SOURCE_CONTROL.getInstance().oneThreadCompleted();
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
        }
        return oc; // if a null is returned, it means, we are done with level!
    }

    protected OperationChain getOC(int threadId, int dLevel) {
        List<OperationChain> ocs = layeredOCBucketGlobal.get(threadId).get(dLevel);
        OperationChain oc = null;
        if (ocs != null && ocs.size() > 0) {
            oc = ocs.remove(ocs.size() - 1);
        }
        return oc;
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
