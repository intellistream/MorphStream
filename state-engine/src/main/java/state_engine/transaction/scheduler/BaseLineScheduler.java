package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class BaseLineScheduler implements IScheduler {

    protected ConcurrentHashMap<Integer, HashMap<Integer, List<OperationChain>>> dLevelBasedOCBuckets;

    protected int[] scheduledOcsCount;
    protected int[] totalOcsToSchedule;
    protected int[] currentDLevelToProcess;

    public BaseLineScheduler(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap(); // TODO: make sure this will not cause trouble with multithreaded access.
        scheduledOcsCount = new int[tp];
        totalOcsToSchedule = new int[tp];
        currentDLevelToProcess = new int[tp];

        for(int threadId=0; threadId<tp; threadId++) {
            dLevelBasedOCBuckets.put(threadId, new HashMap<>());
        }
    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

        totalOcsToSchedule[threadId] += ocs.size();
        HashMap<Integer, List<OperationChain>> currentThreadOCsBucket = dLevelBasedOCBuckets.get(threadId);
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if(!currentThreadOCsBucket.containsKey(dLevel))
                currentThreadOCsBucket.put(dLevel, new ArrayList<>());
            currentThreadOCsBucket.get(dLevel).add(oc);
        }

    }

    @Override
    public OperationChain next(int threadId) {
        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);

        if(oc!=null) {
            scheduledOcsCount[threadId] += 1;
        } else if(!areAllOCsScheduled(threadId)){
            while(oc==null){
                currentDLevelToProcess[threadId]+=1;
                oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);

                MeasureTools.BEGIN_BARRIER_TIME_MEASURE(threadId);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                SOURCE_CONTROL.getInstance().updateThreadBarrierOnDLevel(currentDLevelToProcess[threadId]);
                MeasureTools.END_BARRIER_TIME_MEASURE(threadId);
            }
            scheduledOcsCount[threadId] += 1;
        } else {
            MeasureTools.BEGIN_BARRIER_TIME_MEASURE(threadId);
            SOURCE_CONTROL.getInstance().oneThreadCompleted();
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            MeasureTools.END_BARRIER_TIME_MEASURE(threadId);
        }
        return oc; // if a null is returned, it means, we are done with level!
    }

    private OperationChain getOcForThreadAndDLevel(int threadId, int dLevel) {
        List<OperationChain> ocs = dLevelBasedOCBuckets.get(threadId).get(dLevel);
        OperationChain oc = null;
        if(ocs!=null && ocs.size()>0) {
            oc = ocs.remove(ocs.size()-1);
        }
        return oc;
    }

    @Override
    public boolean areAllOCsScheduled(int threadId){
        return scheduledOcsCount[threadId] == totalOcsToSchedule[threadId];
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
        for(int lop = 0; lop< totalOcsToSchedule.length; lop++) {
            totalOcsToSchedule[lop] = 0;
            scheduledOcsCount[lop] = 0;
            currentDLevelToProcess[lop] = 0;
        }
    }

}
