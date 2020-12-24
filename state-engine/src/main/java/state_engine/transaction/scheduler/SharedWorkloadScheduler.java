package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SharedWorkloadScheduler implements IScheduler {

    protected ConcurrentHashMap<Integer, List<OperationChain>> dLevelBasedOCBuckets;

    protected int[] currentDLevelToProcess;
    protected int totalThreads;
    protected Integer maxDLevel;

    public SharedWorkloadScheduler(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap();
        currentDLevelToProcess = new int[tp];
        totalThreads = tp;
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

    protected OperationChain getOcForThreadAndDLevel(int threadId, int dLevel) {
        List<OperationChain> ocs = dLevelBasedOCBuckets.get(dLevel);
        OperationChain oc = null;
        if(ocs!=null)
            synchronized (ocs) {
                if(ocs.size()>0)
                    oc = ocs.remove(ocs.size()-1); // TODO: This might be costly, maybe we should stop removing and using a counter or use a synchronized queue?
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
        dLevelBasedOCBuckets.clear();
        for(int lop=0; lop<currentDLevelToProcess.length; lop++) {
            currentDLevelToProcess[lop] = 0;
        }
        maxDLevel = 0;
    }

}
