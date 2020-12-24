package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// schedules Ocs in round robin fashion for each level.
public class RoundRobinScheduler implements IScheduler {

    private ConcurrentHashMap<Integer, List<OperationChain>> dLevelBasedOCBuckets;

    private int[] currentDLevelToProcess;
    private int[] indexOfNextOCToProcess;

    private int totalThreads;
    private int maxDLevel;

    public RoundRobinScheduler(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap();
        currentDLevelToProcess = new int[tp];
        indexOfNextOCToProcess = new int[tp];

        totalThreads = tp;
        for(int threadId=0; threadId<tp; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
        }

    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if(dLevel>maxDLevel)
                maxDLevel = dLevel;

            synchronized (this) { // TODO: find an efficient way to merge ocs from all threads into a shared data structure.
                if(!dLevelBasedOCBuckets.containsKey(dLevel))
                    dLevelBasedOCBuckets.put(dLevel, new ArrayList<>());
                dLevelBasedOCBuckets.get(dLevel).add(oc);
            }
        }

    }


    @Override
    public OperationChain next(int threadId) {

        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        if(oc!=null)
            return oc;

        if(!areAllOCsScheduled(threadId)) {
            while(oc==null) {
                currentDLevelToProcess[threadId]+=1;
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

    private OperationChain getOcForThreadAndDLevel(int threadId, int dLevel) {
        List<OperationChain> ocs = dLevelBasedOCBuckets.get(dLevel);
        OperationChain oc = null;
        int indexOfOC = indexOfNextOCToProcess[threadId];
        if(ocs!=null && ocs.size()>indexOfOC) {
            oc = ocs.get(indexOfOC);
        }
        if(oc!=null)
            indexOfNextOCToProcess[threadId] = indexOfOC + totalThreads;

        return oc;
    }

    @Override
    public boolean areAllOCsScheduled(int threadId){
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

        for(int threadId=0; threadId<totalThreads; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
        }

        for(int lop=0; lop<currentDLevelToProcess.length; lop++) {
            currentDLevelToProcess[lop] = 0;
        }

        maxDLevel = 0;
    }

}
