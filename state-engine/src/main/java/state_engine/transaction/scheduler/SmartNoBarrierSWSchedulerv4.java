package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartNoBarrierSWSchedulerv4 implements IScheduler, OperationChain.IOnDependencyResolvedListener {


    private Object leftOversLock = new Object();
    private OperationChain leftOversHead;
    private OperationChain leftOversTail;
    private OperationChain leftOverCurrent;

    private Object withDependentsLock = new Object();
    private OperationChain withDependentsHead;
    private OperationChain withDependentsTail;
    private OperationChain withDependentsCurrent;

    private AtomicInteger totalSubmitted;
    private AtomicInteger totalProcessed;

    private OperationChain[] leftOversLocalCacheHead;
    private OperationChain[] leftOversLocalCacheTail;
    private OperationChain[] withDependentsLocalCacheHead;
    private OperationChain[] withDependentsLocalCacheTail;

    public SmartNoBarrierSWSchedulerv4(int tp) {
        totalSubmitted = new AtomicInteger(0);
        totalProcessed = new AtomicInteger(0);

        leftOversLocalCacheHead = new OperationChain[tp];
        leftOversLocalCacheTail = new OperationChain[tp];

        withDependentsLocalCacheHead = new OperationChain[tp];
        withDependentsLocalCacheTail = new OperationChain[tp];
    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

        OperationChain loHead = null;
        OperationChain loTail = null;

        OperationChain wdHead = null;
        OperationChain wdTail = null;

        for (OperationChain oc : ocs) {
            if(!oc.hasDependency() && oc.hasDependents())
                if(loTail!=null) {
                    loTail.next = oc;
                    oc.prev = loTail;
                    loTail = oc;
                } else {
                    loHead = oc;
                    loTail = oc;
                }
            else if(!oc.hasDependency())
                if(wdTail!=null) {
                    wdTail.next = oc;
                    oc.prev = wdTail;
                    wdTail = oc;
                } else {
                    wdHead = oc;
                    wdTail = oc;
                }
            else
                oc.setOnOperationChainChangeListener(this);
        }

        totalSubmitted.addAndGet(ocs.size());

        synchronized (leftOversLock) {
            if (leftOversHead == null) {
                leftOversHead = loHead;
                leftOversTail = loTail;
                leftOverCurrent = leftOversHead;
            } else {
                leftOversTail.next = loHead;
                loHead.prev = leftOversTail;
                leftOversTail = loTail;
            }
        }

        synchronized (withDependentsLock) {
            if (withDependentsHead == null) {
                withDependentsHead = wdHead;
                withDependentsTail = wdTail;
                withDependentsCurrent = withDependentsHead;
            } else {
                withDependentsTail.next = wdHead;
                wdHead.prev = withDependentsTail;
                withDependentsTail = wdTail;
            }
        }
    }


    @Override
    public void onDependencyResolvedListener(int threadId, OperationChain oc) {
        if(oc.hasDependents())
            if(withDependentsLocalCacheTail[threadId]!=null) {
                withDependentsLocalCacheTail[threadId].next = oc;
                oc.prev = withDependentsLocalCacheTail[threadId];
                withDependentsLocalCacheTail[threadId] = oc;
            } else {
                withDependentsLocalCacheHead[threadId] = oc;
                withDependentsLocalCacheTail[threadId] = oc;
            }
        else
            if(leftOversLocalCacheTail[threadId] != null) {
                leftOversLocalCacheTail[threadId].next = oc;
                oc.prev = leftOversLocalCacheTail[threadId];
                leftOversLocalCacheTail[threadId] = oc;
            } else {
                leftOversLocalCacheHead[threadId] = oc;
                leftOversLocalCacheTail[threadId] = oc;
            }


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

    protected OperationChain getOcForThreadAndDLevel(int threadId) {
        OperationChain oc = null;

        synchronized (withDependentsLock) {

            if(withDependentsCurrent!=null) {
                oc = withDependentsCurrent;
                withDependentsCurrent = withDependentsCurrent.next;
            }

            if(oc==null && withDependentsLocalCacheHead[threadId] != null) {

                oc = withDependentsLocalCacheHead[threadId];
                withDependentsLocalCacheHead[threadId] = withDependentsLocalCacheHead[threadId].next;

                if(withDependentsLocalCacheHead[threadId] != null) {
                    withDependentsTail.next = withDependentsLocalCacheHead[threadId];
                    withDependentsLocalCacheHead[threadId].prev = withDependentsTail;
                    withDependentsTail = withDependentsLocalCacheTail[threadId];

                    if(withDependentsCurrent==null)
                        withDependentsCurrent = withDependentsLocalCacheHead[threadId];
                }

                withDependentsLocalCacheHead[threadId] = null;
                withDependentsLocalCacheTail[threadId] = null;
            }
        }


        synchronized (leftOversLock) {

            if(oc==null) {
                if(leftOverCurrent!=null) {
                    oc = leftOverCurrent;
                    leftOverCurrent = leftOverCurrent.next;
                }
            }

            if(oc==null && leftOversLocalCacheHead[threadId] != null) {

                oc = leftOversLocalCacheHead[threadId];
                leftOversLocalCacheHead[threadId] = leftOversLocalCacheHead[threadId].next;

                if(leftOversLocalCacheHead[threadId] != null) {
                    leftOversTail.next = leftOversLocalCacheHead[threadId];
                    leftOversLocalCacheHead[threadId].prev = leftOversTail;
                    leftOversTail = leftOversLocalCacheTail[threadId];

                    if(leftOverCurrent==null)
                        leftOverCurrent = leftOversLocalCacheHead[threadId];
                }

                leftOversLocalCacheHead[threadId] = null;
                leftOversLocalCacheTail[threadId] = null;
            }
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
        leftOversHead = null;
        leftOversTail = null;
        leftOverCurrent = null;

        withDependentsHead = null;
        withDependentsTail = null;
        withDependentsCurrent = null;

        totalSubmitted = new AtomicInteger(0);
        totalProcessed = new AtomicInteger(0);
    }

}
