package transaction.scheduler;

import common.OperationChain;
import profiler.MeasureTools;
import utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
/**
 * Author: Aqif Hamid
 * Concrete impl of barriered shared workload scheduler
 */
public class SharedWorkloadScheduler implements IScheduler {

    protected ConcurrentHashMap<Integer, Queue<OperationChain>> dLevelBasedOCBuckets;

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
                dLevelBasedOCBuckets.putIfAbsent(dLevel, new ConcurrentLinkedQueue<>());

        for (int dLevel : dLevelBasedOCBucketsPerThread.keySet()) {
            Queue<OperationChain> dLevelList = dLevelBasedOCBuckets.get(dLevel);
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
                oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
                MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
            }
        }

        return oc;
    }

    protected OperationChain getOcForThreadAndDLevel(int threadId, int dLevel) {
        Queue<OperationChain> ocs = dLevelBasedOCBuckets.get(dLevel);
        OperationChain oc = null;
        if (ocs != null)
            oc = ocs.poll(); // TODO: This might be costly, maybe we should stop removing and using a counter or use a synchronized queue?
        return oc;
    }

    @Override
    public boolean finishedScheduling(int threadId) {
        return currentDLevelToProcess[threadId] == maxDLevel &&
                dLevelBasedOCBuckets.get(maxDLevel).isEmpty();
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
        for (int lop = 0; lop < currentDLevelToProcess.length; lop++) {
            currentDLevelToProcess[lop] = 0;
        }
        maxDLevel = 0;
    }

}
