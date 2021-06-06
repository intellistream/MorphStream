package transaction.scheduler.layered.nonhashed;

import common.OperationChain;
import profiler.MeasureTools;
import utils.SOURCE_CONTROL;

import java.util.*;

/**
 * Author: Aqif Hamid
 * Concrete impl of Layered round robin scheduler
 */
public class LayeredRoundRobinScheduler extends LayeredNonHashScheduler<List<OperationChain>> {

    protected int[] indexOfNextOCToProcess;

    public LayeredRoundRobinScheduler(int tp) {
        super(tp);
        indexOfNextOCToProcess = new int[tp];
        totalThreads = tp;
        for (int threadId = 0; threadId < tp; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
        }
    }

    @Override
    public void submitOperationChains(int threadId, Collection<OperationChain> ocs) {
        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = buildTempBucketPerThread(threadId, ocs);

        for (int dLevel : layeredOCBucketThread.keySet())
            if (!layeredOCBucketGlobal.containsKey(dLevel))
                layeredOCBucketGlobal.putIfAbsent(dLevel, new ArrayList<>());

        for (int dLevel : layeredOCBucketThread.keySet()) {
            List<OperationChain> dLevelList = layeredOCBucketGlobal.get(dLevel);
            MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
            synchronized (dLevelList) {
                MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
                dLevelList.addAll(layeredOCBucketThread.get(dLevel));
            }
            layeredOCBucketThread.get(dLevel).clear();
        }
    }

    /**
     * @param threadId
     * @param dLevel
     * @return
     */
    protected OperationChain getOC(int threadId, int dLevel) {
        List<OperationChain> ocs = layeredOCBucketGlobal.get(dLevel);
        OperationChain oc = null;
        int indexOfOC = indexOfNextOCToProcess[threadId];
        if (ocs != null) {
            if (ocs.size() > indexOfOC) {
                oc = ocs.get(indexOfOC);
                indexOfNextOCToProcess[threadId] = indexOfOC + totalThreads;
            } else {
                indexOfNextOCToProcess[threadId] = indexOfOC - ocs.size();
            }
        }
        return oc;
    }

    @Override
    public boolean finishedScheduling(int threadId) {
        return currentLevel[threadId] > maxDLevel;
    }

    @Override
    public void reset() {
        layeredOCBucketGlobal.clear();
        for (int threadId = 0; threadId < totalThreads; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
            currentLevel[threadId] = 0;
        }
        maxDLevel = 0;
    }

}
