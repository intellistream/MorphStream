package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SmartSharedWorkloadv1 implements IScheduler {

    private ConcurrentHashMap<Integer, List<OperationChain>> dLevelBasedOCBuckets;
    private int[] currentDLevelToProcess;

    private int totalThreads;
    private int maxDLevel;

    public SmartSharedWorkloadv1(int tp) {
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

            synchronized (this) { // TODO: find an efficient way to merge ocs from all threads into a shared data structure.
                if(!dLevelBasedOCBuckets.containsKey(dLevel))
                    dLevelBasedOCBuckets.put(dLevel, new ArrayList<>());
                insertInOrder(oc, oc.getIndependentOpsCount(), dLevelBasedOCBuckets.get(dLevel));
            }

        }
    }

    public synchronized void insertInOrder(OperationChain oc, int priority, List<OperationChain> operationChains) {
        if(operationChains.size() == 0 || priority > operationChains.get(operationChains.size()-1).getPriority()) {
            operationChains.add(oc);
            return;
        }

        int insertionIndex, center;
        int start = 0;
        int end = operationChains.size()-1;

        while(true) {
            center = (start+end)/2;
            if(center==start || center == end) {
                insertionIndex = start;
                break;
            }
            if(priority <= operationChains.get(center).getPriority())
                end = center;
            else
                start = center;
        }

        while(operationChains.get(insertionIndex).getPriority() < priority)
            insertionIndex++;

        while(operationChains.get(insertionIndex).getPriority() == priority &&
                operationChains.get(insertionIndex).getIndependentOpsCount() > oc.getIndependentOpsCount()) {
            insertionIndex++;
            if(insertionIndex==operationChains.size())
                break;
        }

        if(insertionIndex==operationChains.size())
            operationChains.add(oc);
        else
            operationChains.add(insertionIndex, oc); // insert using second level priority
    }



    @Override
    public OperationChain next(int threadId) {
        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        while(oc==null) {
            if(areAllOCsScheduled(threadId))
                break;
            currentDLevelToProcess[threadId]+=1;
            oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        }
        while(oc!=null && oc.hasDependency());
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
