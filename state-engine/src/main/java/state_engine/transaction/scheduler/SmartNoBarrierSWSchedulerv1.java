package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class SmartNoBarrierSWSchedulerv1 extends NoBarrierSharedWorkload {

    private HashMap<Integer, HashMap<Integer, List<OperationChain>>> dLevelBasedOCBucketsPerThread;
    private int maxDLevelPerThread[];

    public SmartNoBarrierSWSchedulerv1(int tp) {
        super(tp);
        dLevelBasedOCBucketsPerThread = new HashMap<>();
        for(int threadId=0; threadId<tp; threadId++) {
            dLevelBasedOCBucketsPerThread.put(threadId, new HashMap<>());
        }
        maxDLevelPerThread = new int[tp];
    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

        int localMaxDLevel = 0;
        HashMap<Integer, List<OperationChain>> currentThreadOCsBucket = dLevelBasedOCBucketsPerThread.get(threadId);
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if(localMaxDLevel < dLevel)
                localMaxDLevel = dLevel;

            if(!currentThreadOCsBucket.containsKey(dLevel))
                currentThreadOCsBucket.put(dLevel, new ArrayList<>());
            currentThreadOCsBucket.get(dLevel).add(oc);
        }

//        for(int lop=0; lop<totalThreads; lop++) {
//            if(maxDLevel < maxDLevelPerThread[lop])
//                maxDLevel = maxDLevelPerThread[lop];
//        }

        synchronized (maxDLevel) {
            if(maxDLevel < localMaxDLevel)
                maxDLevel = localMaxDLevel;
        }

        MeasureTools.BEGIN_BARRIER_TIME_MEASURE(threadId);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
        MeasureTools.END_BARRIER_TIME_MEASURE(threadId);

        for(int dLevel = threadId; dLevel<=maxDLevel; dLevel+=totalThreads) {
            if(!dLevelBasedOCBuckets.containsKey(dLevel))
                dLevelBasedOCBuckets.put(dLevel, new ArrayList<>());
            for(int localThreadId=0; localThreadId<totalThreads; localThreadId++) {
                ocs = dLevelBasedOCBucketsPerThread.get(localThreadId).get(dLevel);
                for(OperationChain oc: ocs)
                    insertInOrder(oc, oc.getIndependentOpsCount(), dLevelBasedOCBuckets.get(dLevel));
            }
        }
    }

    public void insertInOrder(OperationChain oc, int priority, List<OperationChain> operationChains) {

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
    public void reset() {
        super.reset();
        dLevelBasedOCBucketsPerThread = new HashMap<>();
        for(int threadId=0; threadId<totalThreads; threadId++) {
            dLevelBasedOCBucketsPerThread.put(threadId, new HashMap<>());
        }
        maxDLevelPerThread = new int[totalThreads];
    }
}
