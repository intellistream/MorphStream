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

public class SmartNoBarrierRRSchedulerv2 implements IScheduler {

    protected ConcurrentHashMap<Integer, List<OperationChain>> dLevelBasedOCBuckets;
    protected ConcurrentHashMap<Integer, List<OperationChain>> dLevelBasedFreeOCBuckets;

    protected int[] currentDLevelToProcess;
    protected int[] currentDLevelToProcessFreeOcs;

    protected int[] indexOfNextOCToProcess;
    protected int[] indexOfNextFreeOCToProcess;

    protected int totalThreads;
    protected Integer maxDLevel;

    public SmartNoBarrierRRSchedulerv2(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap();
        dLevelBasedFreeOCBuckets = new ConcurrentHashMap();

        currentDLevelToProcess = new int[tp];
        currentDLevelToProcessFreeOcs = new int[tp];
        indexOfNextOCToProcess = new int[tp];
        indexOfNextFreeOCToProcess = new int[tp];

        totalThreads = tp;
        for(int threadId=0; threadId<tp; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
            indexOfNextFreeOCToProcess[threadId] = threadId;
        }
        maxDLevel = 0;
    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

        int localMaxDLevel = 0;

        HashMap<Integer, List<OperationChain>> dLevelBasedOCBucketsPerThread = new HashMap<>();
        HashMap<Integer, List<OperationChain>> dLevelBasedFreeOCBucketsPerThread = new HashMap<>();

        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if(localMaxDLevel < dLevel)
                localMaxDLevel = dLevel;

            if(oc.hasDependents()) {
                if(!dLevelBasedOCBucketsPerThread.containsKey(dLevel))
                    dLevelBasedOCBucketsPerThread.put(dLevel, new ArrayList<>());
                dLevelBasedOCBucketsPerThread.get(dLevel).add(oc);
            } else {
                if(!dLevelBasedFreeOCBucketsPerThread.containsKey(dLevel))
                    dLevelBasedFreeOCBucketsPerThread.put(dLevel, new ArrayList<>());
                dLevelBasedFreeOCBucketsPerThread.get(dLevel).add(oc);
            }

        }

        MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
        synchronized (maxDLevel) {
            MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
            if(maxDLevel < localMaxDLevel)
                maxDLevel = localMaxDLevel;
        }

        for(int dLevel = 0; dLevel <= localMaxDLevel; dLevel++) {

            if(!dLevelBasedOCBuckets.containsKey(dLevel))
                dLevelBasedOCBuckets.putIfAbsent(dLevel, new ArrayList<>());

            if(!dLevelBasedFreeOCBuckets.containsKey(dLevel))
                dLevelBasedFreeOCBuckets.putIfAbsent(dLevel, new ArrayList<>());

            List<OperationChain> ocsLst = dLevelBasedOCBucketsPerThread.get(dLevel);
            if(ocsLst!=null) {
                List<OperationChain> dLevelList = dLevelBasedOCBuckets.get(dLevel);
                MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
                synchronized (dLevelList) {
                    MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
                    dLevelList.addAll(ocsLst);
                }
            }

            ocsLst = dLevelBasedFreeOCBucketsPerThread.get(dLevel);
            if(ocsLst!=null) {
                List<OperationChain> dLevelList = dLevelBasedFreeOCBuckets.get(dLevel);
                MeasureTools.BEGIN_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
                synchronized (dLevelList) {
                    MeasureTools.END_SUBMIT_OVERHEAD_TIME_MEASURE(threadId);
                    dLevelList.addAll(ocsLst);
                }
            }

        }

    }

    @Override
    public OperationChain next(int threadId) {
        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        while(oc==null) {
            if(areAllOCsScheduled(threadId))
                break;

            currentDLevelToProcess[threadId] += 1;
            indexOfNextOCToProcess[threadId] = threadId;

            oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        }

        if(currentDLevelToProcessFreeOcs[threadId] <= currentDLevelToProcess[threadId] && (oc==null || oc.hasDependency())) {

            OperationChain freeOc = getFreeOcForThreadAndDLevel(threadId, currentDLevelToProcessFreeOcs[threadId]);
            while(freeOc==null) {

                if(currentDLevelToProcessFreeOcs[threadId] == currentDLevelToProcess[threadId]
                || areAllFreeOCsScheduled(threadId))

                currentDLevelToProcessFreeOcs[threadId] += 1;
                indexOfNextFreeOCToProcess[threadId] = threadId;

                freeOc = getFreeOcForThreadAndDLevel(threadId, currentDLevelToProcessFreeOcs[threadId]);
            }
            if(freeOc!=null) {
                oc = freeOc;
                indexOfNextOCToProcess[threadId] = indexOfNextOCToProcess[threadId] - totalThreads; // reverse increment
            }
        }

        MeasureTools.BEGIN_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        while(oc!=null && oc.hasDependency());
        MeasureTools.END_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
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

    protected OperationChain getFreeOcForThreadAndDLevel(int threadId, int dLevel) {
        List<OperationChain> ocs = dLevelBasedFreeOCBuckets.get(dLevel);
        OperationChain oc = null;
        int indexOfOC = indexOfNextFreeOCToProcess[threadId];
        if(ocs!=null && ocs.size()>indexOfOC) {
            oc = ocs.get(indexOfOC);
        }
        indexOfNextFreeOCToProcess[threadId] = indexOfOC + totalThreads;

        return oc;
    }

    @Override
    public synchronized boolean areAllOCsScheduled(int threadId) {
        return currentDLevelToProcess[threadId] == maxDLevel &&
                indexOfNextOCToProcess[threadId] >= dLevelBasedOCBuckets.get(maxDLevel).size();
    }

    public synchronized boolean areAllFreeOCsScheduled(int threadId) {
        return currentDLevelToProcessFreeOcs[threadId] == maxDLevel &&
                indexOfNextFreeOCToProcess[threadId] >= dLevelBasedFreeOCBuckets.get(maxDLevel).size();
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
