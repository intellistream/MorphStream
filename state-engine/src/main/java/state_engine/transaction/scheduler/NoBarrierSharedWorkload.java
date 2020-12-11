package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NoBarrierSharedWorkload implements IScheduler {
    private ConcurrentHashMap<Integer, List<OperationChain>> dLevelBasedOCBuckets;

    private int[] currentDLevelToProcess;

    private int totalThreads;
    private int maxDLevel;

    public NoBarrierSharedWorkload(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap();
        currentDLevelToProcess = new int[tp];
        totalThreads = tp;
    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if(dLevel>maxDLevel)
                maxDLevel = dLevel;

            if(!dLevelBasedOCBuckets.containsKey(dLevel))
                dLevelBasedOCBuckets.put(dLevel, new ArrayList<>());
            dLevelBasedOCBuckets.get(dLevel).add(oc);
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
                currentDLevelToProcess[threadId]+=1;
                oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
                MeasureTools.BEGIN_BARRIER_TIME_MEASURE(threadId);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                SOURCE_CONTROL.getInstance().updateThreadBarrierOnDLevel(currentDLevelToProcess[threadId]);
                MeasureTools.END_BARRIER_TIME_MEASURE(threadId);
            }
        }

        if(areAllOCsScheduled(threadId)) {
            MeasureTools.BEGIN_BARRIER_TIME_MEASURE(threadId);
            SOURCE_CONTROL.getInstance().oneThreadCompleted();
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            MeasureTools.END_BARRIER_TIME_MEASURE(threadId);
        }

        return oc;
    }

    private synchronized OperationChain getOcForThreadAndDLevel(int threadId, int dLevel) {
        List<OperationChain> ocs = dLevelBasedOCBuckets.get(dLevel);
        OperationChain oc = null;
        if(ocs!=null && ocs.size()>0) {
            oc = ocs.remove(ocs.size()-1);
        }
        return oc;
    }

    @Override
    public synchronized boolean areAllOCsScheduled(int threadId){
        return currentDLevelToProcess[threadId] == maxDLevel &&
                dLevelBasedOCBuckets.get(maxDLevel).size()==0;
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
        for(int lop=0; lop<currentDLevelToProcess.length; lop++) {
            currentDLevelToProcess[lop] = 0;
        }
        maxDLevel = 0;
    }
}
