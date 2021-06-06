package transaction.scheduler.layered.nonhashed;

import common.OperationChain;
import profiler.MeasureTools;
import utils.SOURCE_CONTROL;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LayeredSharedWorkloadScheduler extends LayeredNonHashScheduler<Queue<OperationChain>> {

    public LayeredSharedWorkloadScheduler(int tp) {
        super(tp);
    }

    @Override
    public void submitOperationChains(int threadId, Collection<OperationChain> ocs) {
        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = buildTempBucketPerThread(threadId, ocs);

        for (int dLevel : layeredOCBucketThread.keySet())
            if (!layeredOCBucketGlobal.containsKey(dLevel))
                layeredOCBucketGlobal.putIfAbsent(dLevel, new ConcurrentLinkedQueue<>());

        for (int dLevel : layeredOCBucketThread.keySet()) {
            Queue<OperationChain> dLevelList = layeredOCBucketGlobal.get(dLevel);
            MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
            synchronized (dLevelList) {
                MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
                dLevelList.addAll(layeredOCBucketThread.get(dLevel));
            }
            layeredOCBucketThread.get(dLevel).clear();
        }
    }

    protected OperationChain getOC(int threadId, int dLevel) {
        Queue<OperationChain> ocs = layeredOCBucketGlobal.get(dLevel);
        OperationChain oc = null;
        if (ocs != null)
            oc = ocs.poll(); // TODO: This might be costly, maybe we should stop removing and using a counter or use a synchronized queue?
        return oc;
    }

    @Override
    public boolean finishedScheduling(int threadId) {
        return currentLevel[threadId] == maxDLevel &&
                layeredOCBucketGlobal.get(maxDLevel).isEmpty();
    }

    @Override
    public void reset() {
        layeredOCBucketGlobal.clear();
        for (int lop = 0; lop < currentLevel.length; lop++) {
            currentLevel[lop] = 0;
        }
        maxDLevel = 0;
    }

}
