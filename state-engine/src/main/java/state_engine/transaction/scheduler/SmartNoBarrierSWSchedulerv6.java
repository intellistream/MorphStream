package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartNoBarrierSWSchedulerv6 implements IScheduler, OperationChain.IOnDependencyResolvedListener {

    private ConcurrentLinkedQueue<OperationChain> leftOvers;
    private ConcurrentLinkedQueue<OperationChain> withDependents;

    private ArrayList<OperationChain>[] leftOversLocal;
    private ArrayList<OperationChain>[] withDependentsLocal;

    private AtomicInteger totalSubmitted;
    private AtomicInteger totalProcessed;

    public SmartNoBarrierSWSchedulerv6(int tp) {
        leftOvers = new ConcurrentLinkedQueue<>();
        withDependents = new ConcurrentLinkedQueue<>();

        leftOversLocal = new ArrayList[tp];
        withDependentsLocal = new ArrayList[tp];
        for(int tId = 0; tId<tp; tId++){
            leftOversLocal[tId] = new ArrayList<>();
            withDependentsLocal[tId] = new ArrayList<>();
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
            withDependentsLocal[threadId].add(oc);
        else
            leftOversLocal[threadId].add(oc);
    }

    @Override
    public OperationChain next(int threadId) {
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

    // each node processes a complete d tree. NOT GOOD! And does not work yet
    protected OperationChain getOcForThreadAndDLevel(int threadId) {

        OperationChain oc = withDependents.poll();
        if(withDependentsLocal[threadId].size() > 0) {
            oc = withDependentsLocal[threadId].remove(withDependentsLocal[threadId].size()-1);
        }

        if(oc == null) {
            oc = withDependents.poll();
            if(withDependentsLocal[threadId].size() > 0)
                withDependents.addAll(withDependentsLocal[threadId]);
            withDependentsLocal[threadId].clear();
        }

        if(oc==null && leftOversLocal[threadId].size() > 0) {
            oc = leftOversLocal[threadId].remove(leftOversLocal[threadId].size()-1);
        }

        if(oc==null) {
            oc = leftOvers.poll();
        }

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
