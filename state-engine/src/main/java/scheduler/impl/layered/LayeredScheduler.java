package scheduler.impl.layered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.context.LayeredTPGContext;
import scheduler.impl.Scheduler;
import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static common.CONTROL.enable_log;
import static java.lang.Integer.min;

public abstract class LayeredScheduler<Context extends LayeredTPGContext<ExecutionUnit, SchedulingUnit>, ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>>
        extends Scheduler<Context, ExecutionUnit, SchedulingUnit> {
    private static final Logger LOG = LoggerFactory.getLogger(LayeredScheduler.class);

    public int targetRollbackLevel = 0;//shared data structure.

    public ConcurrentLinkedDeque<ExecutionUnit> failedOperations;//aborted operations per thread.
    public AtomicBoolean needAbortHandling = new AtomicBoolean(false);//if any operation is aborted during processing.

    public LayeredScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
        this.failedOperations = new ConcurrentLinkedDeque<>();
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
                failedOperations.push(operation);
                needAbortHandling.compareAndSet(false,true);
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
        ResumeExecution(context);
    }

    private void ResumeExecution(Context context) {
        context.rollbackLevel = -1;
        context.isRollbacked = false;
//        if (context.thisThreadId == 0) { // TODO: what should we do to optimize this part?
        if (needAbortHandling.compareAndSet(true, false)) {
            failedOperations.clear();
        }
//        }
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
            if (markAny && context.rollbackLevel == -1) { // current layer contains operations to abort, try to abort this layer.
                context.rollbackLevel = curLevel;
            }
        }
        if (context.rollbackLevel == -1 || context.rollbackLevel > context.currentLevel) { // the thread does not contain aborted operations
            context.rollbackLevel = context.currentLevel;
        }
        context.isRollbacked = true;
        if (enable_log) LOG.debug("enter this method: " + context.thisThreadId);
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
        for (ExecutionUnit failedOp : failedOperations) {
            if (bid == failedOp.bid) {
                operation.aborted = true;
                markAny = true;
            }
        }
        return markAny;
    }

    private void IdentifyRollbackLevel(Context context) {
        if (context.thisThreadId == 0) {
            targetRollbackLevel = Integer.MAX_VALUE;
            for (int i = 0; i < context.totalThreads; i++) { // find the first level that contains aborted operations
                if (enable_log) LOG.debug("is thread rollbacked: " + threadToContextMap.get(i).thisThreadId + " | " + threadToContextMap.get(i).isRollbacked);
                targetRollbackLevel = min(targetRollbackLevel, threadToContextMap.get(i).rollbackLevel);
            }
        }
    }

    private void SetRollbackLevel(Context context) {
        if (enable_log) LOG.debug("++++++ rollback at: " + targetRollbackLevel);
        context.rollbackLevel = targetRollbackLevel;
    }

    private void RollbackToCorrectLayerForRedo(Context context) {
        int level;
        for (level = context.rollbackLevel; level <= context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        // it needs to rollback to the level -1, because aborthandling has immediately followed up with ProcessedToNextLevel
        context.currentLevel = context.rollbackLevel-1;
    }

    private int getNumOPsByLevel(Context context, int level) {
        int ops = 0;
        if (context.allocatedLayeredOCBucket.containsKey(level)) { // oc level may not be sequential
            for (SchedulingUnit operationChain : context.allocatedLayeredOCBucket.get(level)) {
                ops += operationChain.getOperations().size();
            }
        }
        return ops;
    }
}
