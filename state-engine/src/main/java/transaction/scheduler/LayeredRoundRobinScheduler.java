package transaction.scheduler;

import common.OperationChain;
import profiler.MeasureTools;
import utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: Aqif Hamid
 * Concrete impl of Layered round robin scheduler
 */
public class LayeredRoundRobinScheduler extends LayeredScheduler<List<OperationChain>> implements IScheduler {

    protected int[] indexOfNextOCToProcess;
    protected int totalThreads;
    protected Integer maxDLevel;

    public LayeredRoundRobinScheduler(int tp) {
        super(tp);
        indexOfNextOCToProcess = new int[tp];
        totalThreads = tp;
        for (int threadId = 0; threadId < tp; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
        }
        maxDLevel = 0;
    }

    @Override
    public void submitOperationChains(int threadId, Collection<OperationChain> ocs) {

        int localMaxDLevel = 0;
        HashMap<Integer, List<OperationChain>> dLevelBasedOCBucketsPerThread = new HashMap<>();

        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if (localMaxDLevel < dLevel)
                localMaxDLevel = dLevel;

            if (!dLevelBasedOCBucketsPerThread.containsKey(dLevel))
                dLevelBasedOCBucketsPerThread.put(dLevel, new ArrayList<>());
            dLevelBasedOCBucketsPerThread.get(dLevel).add(oc);
        }

        MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
        synchronized (maxDLevel) {
            MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
            if (maxDLevel < localMaxDLevel)
                maxDLevel = localMaxDLevel;
        }

        for (int dLevel : dLevelBasedOCBucketsPerThread.keySet())
            if (!dLevelBasedOCBuckets.containsKey(dLevel))
                dLevelBasedOCBuckets.putIfAbsent(dLevel, new ArrayList<>());

        for (int dLevel : dLevelBasedOCBucketsPerThread.keySet()) {
            List<OperationChain> dLevelList = dLevelBasedOCBuckets.get(dLevel);
            MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
            synchronized (dLevelList) {
                MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
                dLevelList.addAll(dLevelBasedOCBucketsPerThread.get(dLevel));
            }
            dLevelBasedOCBucketsPerThread.get(dLevel).clear();
        }

    }

    @Override
    public OperationChain nextOperationChain(int threadId) {

        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        if (oc != null)
            return oc;

        if (!finishedScheduling(threadId)) {
            while (oc == null) {
                if (finishedScheduling(threadId))
                    break;
                currentDLevelToProcess[threadId] += 1;
//                indexOfNextOCToProcess[threadId] = threadId;
                oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);

                MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
            }
        }

        return oc;
    }

    protected OperationChain getOcForThreadAndDLevel(int threadId, int dLevel) {
        List<OperationChain> ocs = dLevelBasedOCBuckets.get(dLevel);
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
        return currentDLevelToProcess[threadId] > maxDLevel;
    }

    @Override
    public void reSchedule(int threadId, OperationChain oc) {
        throw new NotImplementedException();
    }

    @Override
    public boolean isReSchedulingEnabled() {
        return false;
    }

    @Override
    public void reset() {

        dLevelBasedOCBuckets.clear();

        for (int threadId = 0; threadId < totalThreads; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
            currentDLevelToProcess[threadId] = 0;
        }

        maxDLevel = 0;
    }

}