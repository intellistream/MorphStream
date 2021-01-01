package state_engine.transaction.scheduler;

import state_engine.common.Operation;
import state_engine.common.OperationChain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartNoBarrierSWSchedulerv21 implements IScheduler, OperationChain.IOnDependencyResolvedListener {

    private ConcurrentLinkedQueue<OperationChain> leftOvers;
    private ConcurrentLinkedQueue<OperationChain> withDependents;

    ArrayList<OperationChain>[] withDependentsScheduled;
    ArrayList<OperationChain>[] withLeftOversScheduled;

    private AtomicInteger totalSubmitted;
    private AtomicInteger totalProcessed;
    private int threadCount = 0;

    public SmartNoBarrierSWSchedulerv21(int tp) {

        threadCount = tp;

        leftOvers = new ConcurrentLinkedQueue<>();
        withDependents = new ConcurrentLinkedQueue<>();
        withDependentsScheduled = new ArrayList[tp];
        withLeftOversScheduled = new ArrayList[tp];

        for(int lop=0; lop<threadCount; lop++) {
            withDependentsScheduled[lop] =  new ArrayList<>();
            withLeftOversScheduled[lop] =  new ArrayList<>();
        }

        totalSubmitted = new AtomicInteger(0);
        totalProcessed = new AtomicInteger(0);
    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

        for (OperationChain oc : ocs) {
            if(!oc.hasDependency() && oc.hasDependents())
                withDependents.add(oc);
            else if(!oc.hasDependency())
                leftOvers.add(oc);
            else
                oc.setOnOperationChainChangeListener(this);
        }
        totalSubmitted.addAndGet(ocs.size());
    }

    @Override
    public void onDependencyResolvedListener(int threadId, OperationChain oc) {
        if(oc.hasDependents())
            withDependentsScheduled[threadId].add(oc);
        else
            withLeftOversScheduled[threadId].add(oc);

        if(withDependents.size() <= threadCount && !withDependentsScheduled[threadId].isEmpty()) {
            withDependents.addAll(withDependentsScheduled[threadId]);
            withDependentsScheduled[threadId].clear();
        }

        if(leftOvers.size() <= threadCount && !withLeftOversScheduled[threadId].isEmpty()) {
            leftOvers.addAll(withLeftOversScheduled[threadId]);
            withLeftOversScheduled[threadId].clear();
        }
    }

    @Override
    public OperationChain next(int threadId) {

        if(withDependents.isEmpty() && !withDependentsScheduled[threadId].isEmpty()) {
            withDependents.addAll(withDependentsScheduled[threadId]);
            withDependentsScheduled[threadId].clear();
        }

        if(leftOvers.isEmpty() && !withLeftOversScheduled[threadId].isEmpty()) {
            leftOvers.addAll(withLeftOversScheduled[threadId]);
            withLeftOversScheduled[threadId].clear();
        }

        OperationChain oc = getOcForThreadAndDLevel(threadId);
        while(oc==null) {
            if(areAllOCsScheduled(threadId))
                break;
            oc = getOcForThreadAndDLevel(threadId);
        }

        if(oc!=null)
            totalProcessed.incrementAndGet();


        return oc;
    }

    protected OperationChain getOcForThreadAndDLevel(int threadId) {
        OperationChain oc = withDependents.poll();
        if(oc==null)
            oc = leftOvers.poll();
        return oc;
    }

    @Override
    public boolean areAllOCsScheduled(int threadId) {
        return totalProcessed.get()==totalSubmitted.get();
    }

    @Override
    public void reSchedule(int threadId, OperationChain oc) {

    }

    @Override
    public boolean isReSchedulingEnabled() {
        return false;
    }


    @Override
    public void reset() {
        leftOvers = new ConcurrentLinkedQueue<>();
        withDependents = new ConcurrentLinkedQueue<>();
        totalSubmitted = new AtomicInteger(0);
        totalProcessed = new AtomicInteger(0);
    }

}
