package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// schedules Ocs in round robin fashion for each level.
public class RoundRobinScheduler implements IScheduler {

    protected volatile ConcurrentHashMap<Integer, List<OperationChain>> dLevelBasedOCBuckets;

    protected int[] currentDLevelToProcess;
    protected int[] indexOfNextOCToProcess;

    protected int totalThreads;
    protected Integer maxDLevel;

    public RoundRobinScheduler(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap();
        currentDLevelToProcess = new int[tp];
        indexOfNextOCToProcess = new int[tp];

        totalThreads = tp;
        for(int threadId=0; threadId<tp; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
        }
        maxDLevel = 0;
    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

        int localMaxDLevel = 0;
        HashMap<Integer, List<OperationChain>> dLevelBasedOCBucketsPerThread = new HashMap<>();

        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if(localMaxDLevel < dLevel)
                localMaxDLevel = dLevel;

            if(!dLevelBasedOCBucketsPerThread.containsKey(dLevel))
                dLevelBasedOCBucketsPerThread.put(dLevel, new ArrayList<>());
            dLevelBasedOCBucketsPerThread.get(dLevel).add(oc);
        }

        // These sync blocks might act like a barrier.
        synchronized (maxDLevel) {
            if(maxDLevel < localMaxDLevel)
                maxDLevel = localMaxDLevel;
        }

        for(int dLevel : dLevelBasedOCBucketsPerThread.keySet())
            dLevelBasedOCBuckets.putIfAbsent(dLevel, new ArrayList<>());

        for(int dLevel : dLevelBasedOCBucketsPerThread.keySet()) {
            List<OperationChain> dLevelList = dLevelBasedOCBuckets.get(dLevel);
            synchronized (dLevelList) {
                dLevelList.addAll(dLevelBasedOCBucketsPerThread.get(dLevel));
            }
            dLevelBasedOCBucketsPerThread.get(dLevel).clear();
        }

    }

    @Override
    public OperationChain next(int threadId) {

        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        if(oc!=null)
            return oc;

        if(!areAllOCsScheduled(threadId)) {
            while(oc==null) {
                if(areAllOCsScheduled(threadId))
                    break;
                currentDLevelToProcess[threadId] += 1;
                indexOfNextOCToProcess[threadId] = threadId;
                oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);

                MeasureTools.BEGIN_BARRIER_TIME_MEASURE(threadId);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                SOURCE_CONTROL.getInstance().updateThreadBarrierOnDLevel(currentDLevelToProcess[threadId]);
                MeasureTools.END_BARRIER_TIME_MEASURE(threadId);
            }
        } else {
            MeasureTools.BEGIN_BARRIER_TIME_MEASURE(threadId);
            SOURCE_CONTROL.getInstance().oneThreadCompleted();
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            MeasureTools.END_BARRIER_TIME_MEASURE(threadId);
        }

        return oc;
    }

    protected OperationChain getOcForThreadAndDLevel(int threadId, int dLevel) {
        List<OperationChain> ocs = dLevelBasedOCBuckets.get(dLevel);
        OperationChain oc = null;
        int indexOfOC = indexOfNextOCToProcess[threadId];
        if(ocs!=null && ocs.size()>indexOfOC) {
            oc = ocs.get(indexOfOC);
        }
        indexOfNextOCToProcess[threadId] = indexOfOC + totalThreads;

        return oc;
    }

    @Override
    public synchronized boolean areAllOCsScheduled(int threadId){
        return currentDLevelToProcess[threadId] == maxDLevel &&
                indexOfNextOCToProcess[threadId] >= dLevelBasedOCBuckets.get(maxDLevel).size();
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

        for(int threadId=0; threadId<totalThreads; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
            currentDLevelToProcess[threadId] = 0;
        }

        maxDLevel = 0;
    }

}
