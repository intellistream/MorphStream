package scheduler.impl.layered;

import profiler.MeasureTools;
import scheduler.context.LayeredTPGContext;
import scheduler.impl.Scheduler;
import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.Map;

import static java.lang.Integer.min;

public abstract class LayeredScheduler<Context extends LayeredTPGContext<ExecutionUnit, SchedulingUnit>, ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>>
        extends Scheduler<Context, ExecutionUnit, SchedulingUnit> {

    public int targetRollbackLevel;//shared data structure.

    public LayeredScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(Context context) {
        int threadId = context.thisThreadId;
        tpg.firstTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        SchedulingUnit next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        if (next != null) {
            execute(context, next.getOperations(), mark_ID);
            NOTIFY(next, context);
        }
    }

    /**
     * Used by BFSScheduler.
     *
     * @param context
     * @param operation_chain
     * @param mark_ID
     */
    public void execute(Context context, MyList<ExecutionUnit> operation_chain, long mark_ID) {
        for (ExecutionUnit operation : operation_chain) {
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
            if (operation.isFailed && !operation.aborted) {
                context.abortedOperations.push(operation);
                context.needAbortHandling = true;
            }
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    private SchedulingUnit next(Context context) {
        SchedulingUnit operationChain = (SchedulingUnit) context.ready_oc;
        context.ready_oc = null;
        return operationChain;// if a null is returned, it means, we are done with this level!
    }

    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     */
    @Override
    public void DISTRIBUTE(SchedulingUnit task, Context context) {
        context.ready_oc = task;
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param context
     * @return
     */
    protected SchedulingUnit Next(Context context) {
        ArrayList<SchedulingUnit> ocs = context.OCSCurrentLayer(); //
        SchedulingUnit oc = null;
        if (ocs != null && context.currentLevelIndex < ocs.size()) {
            oc = ocs.get(context.currentLevelIndex++);
            context.scheduledOPs += oc.getOperations().size();
        }
        return oc;
    }

    /********************abort handling methods********************/

    protected void abortHandling(Context context) {
        MarkOperationsToAbort(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        IdentifyRollbackLevel(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        SetRollbackLevel(context);
        RollbackToCorrectLayerForRedo(context);
        context.needAbortHandling = false;
    }

    //TODO: mark operations of aborted transaction to be aborted.
    private void MarkOperationsToAbort(Context context) {
        boolean markAny = false;
        ArrayList<SchedulingUnit> operationChains;
        int curLevel;
        for (Map.Entry<Integer, ArrayList<SchedulingUnit>> operationChainsEntry : context.allocatedLayeredOCBucket.entrySet()) {
            operationChains = operationChainsEntry.getValue();
            curLevel = operationChainsEntry.getKey();
            for (SchedulingUnit operationChain : operationChains) {
                for (ExecutionUnit operation : operationChain.getOperations()) {
                    markAny |= _MarkOperationsToAbort(context, operation);
                }
            }
//            if (!markAny) {//current layer no one being marked.
//                context.rollbackLevel++;
//            }
            if (markAny) { // current layer contains operations to abort, try to abort the this layer.
                context.rollbackLevel = curLevel;
                break;
            }
        }
        context.rollbackLevel = min(context.rollbackLevel, context.currentLevel);
        System.out.println(context.thisThreadId + " | " + context.rollbackLevel);
    }

    /**
     * Mark operations of an aborted transaction to abort.
     *
     * @param context
     * @param operation
     * @return
     */
    private boolean _MarkOperationsToAbort(Context context, ExecutionUnit operation) {
        long bid = operation.bid;
        boolean markAny = false;
        //identify bids to be aborted.
        for (ExecutionUnit op : context.abortedOperations) {
            if (bid == op.bid) {
                op.aborted = true;
                markAny = true;
            }
        }
        return markAny;
    }

    private void IdentifyRollbackLevel(Context context) {
        if (context.thisThreadId == 0) {
            targetRollbackLevel = Integer.MAX_VALUE;
            for (int i = 0; i < context.totalThreads; i++) {
                targetRollbackLevel = min(targetRollbackLevel, threadToContextMap.get(i).rollbackLevel);
            }
        }
    }

    private void SetRollbackLevel(Context context) {
        System.out.println("rollback at: " + targetRollbackLevel);
        context.rollbackLevel = targetRollbackLevel;
    }

    private void RollbackToCorrectLayerForRedo(Context context) {
        int level;
        for (level = context.rollbackLevel; level < context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        context.currentLevel = context.rollbackLevel;
        context.rollbackLevel = 0;
    }

    private int getNumOPsByLevel(Context context, int level) {
        int ops = 0;
        if (context.allocatedLayeredOCBucket.containsKey(level)) {
            for (SchedulingUnit operationChain : context.allocatedLayeredOCBucket.get(level)) {
                ops += operationChain.getOperations().size();
            }
        }
        return ops;
    }
}
