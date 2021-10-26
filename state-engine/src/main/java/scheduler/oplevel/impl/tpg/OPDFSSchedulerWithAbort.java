package scheduler.oplevel.impl.tpg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.context.DFSLayeredTPGContextWithAbort;
import scheduler.oplevel.context.OPLayeredContextWithAbort;
import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;
import scheduler.struct.layered.dfs.DFSOperation;
import scheduler.struct.layered.dfs.DFSOperationChain;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static common.CONTROL.enable_log;

public class OPDFSSchedulerWithAbort<Context extends OPLayeredContextWithAbort> extends OPDFSScheduler<Context> {
    private static final Logger LOG = LoggerFactory.getLogger(OPDFSSchedulerWithAbort.class);

    public final ConcurrentLinkedDeque<Operation> failedOperations = new ConcurrentLinkedDeque<>();//aborted operations per thread.
    public final AtomicBoolean needAbortHandling = new AtomicBoolean(false);//if any operation is aborted during processing.

    public OPDFSSchedulerWithAbort(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    /**
     * // O1 -> (logical)  O2
     * // T1: pickup O1. Transition O1 (ready - > execute) || notify O2 (speculative -> ready).
     * // T2: pickup O2 (speculative -> executed)
     * // T3: pickup O2
     * fast explore dependencies in TPG and put ready/speculative operations into task queues.
     *
     * @param context
     */
    @Override
    public void EXPLORE(Context context) {
        Operation op = Next(context);
        while (op == null) {
            // current thread finishes the current level
            if (needAbortHandling.get()) {
                abortHandling(context);
            }
            if (context.finished())
                break;
            ProcessedToNextLevel(context);
            op = Next(context);
        }
        while (op != null && op.hasParents()) {
            // Busy-Wait for dependency resolution
            if (needAbortHandling.get()) {
                return; // skip this busy wait when abort happens
            }
        }
        DISTRIBUTE(op, context);
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int cnt = 0;
        int batch_size = 100;//TODO;
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);

        do {
            Operation next = next(context);
            if (next == null) {
                break;
            }
            context.batchedOperations.push(next);
            cnt++;
            if (cnt > batch_size) {
                break;
            }
        } while (true);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

//        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        for (Operation operation : context.batchedOperations) {
            execute(operation, mark_ID, false);
        }

        while (context.batchedOperations.size() != 0) {
            Operation remove = context.batchedOperations.remove();
            if (!remove.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
                MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
                NOTIFY(remove, context); // only notify when the operation is executed
                MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
            }
            checkTransactionAbort(remove);
        }
//        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
    }

    protected void checkTransactionAbort(Operation operation) {
        if (operation.isFailed && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            for (Operation failedOp : failedOperations) {
                if (failedOp.bid == operation.bid) { // avoid duplicate abort notification
                    return;
                }
            }
            needAbortHandling.compareAndSet(false, true);
            failedOperations.push(operation); // operation need to wait until the last abort has completed
        }
    }

    protected void abortHandling(Context context) {
        MarkOperationsToAbort(context);
        RollbackToCorrectLayerForRedo(context);
        ResumeExecution(context);
    }

    protected void MarkOperationsToAbort(Context context) {
        boolean markAny = false;
        ArrayList<Operation> operations;
        int curLevel;
        for (Map.Entry<Integer, ArrayList<Operation>> operationsEntry: context.allocatedLayeredOCBucket.entrySet()) {
            operations = operationsEntry.getValue();
            curLevel = operationsEntry.getKey();
            for (Operation operation : operations) {
                markAny |= _MarkOperationsToAbort(context, operation);
            }
            if (markAny && context.rollbackLevel == -1) { // current layer contains operations to abort, try to abort this layer.
                context.rollbackLevel = curLevel;
            }
        }
        if (context.rollbackLevel == -1 || context.rollbackLevel > context.currentLevel) { // the thread does not contain aborted operations
            context.rollbackLevel = context.currentLevel;
        }
        context.isRollbacked = true;
        if (enable_log) LOG.info("++++++ rollback at level: " + context.thisThreadId + " | " + context.rollbackLevel);
    }

    /**
     * Mark operations of an aborted transaction to abort.
     *
     * @param context
     * @param operation
     * @return
     */
    private boolean _MarkOperationsToAbort(Context context, Operation operation) {
        long bid = operation.bid;
        boolean markAny = false;
        //identify bids to be aborted.
        for (Operation failedOp : failedOperations) {
            if (bid == failedOp.bid) {
                operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
                notifyChildren(operation, MetaTypes.OperationStateType.ABORTED);
                markAny = true;
            }
        }
        return markAny;
    }

    protected void ResumeExecution(Context context) {
        context.rollbackLevel = -1;
        context.isRollbacked = false;

        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        needAbortHandling.compareAndSet(true, false);
        failedOperations.clear();
        LOG.info("resume: " + context.thisThreadId);
    }

    protected void RollbackToCorrectLayerForRedo(Context context) {
        int level;
        for (level = context.rollbackLevel; level <= context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        // it needs to rollback to the level -1, because aborthandling has immediately followed up with ProcessedToNextLevel
        context.currentLevel = context.rollbackLevel - 1;
    }

    protected int getNumOPsByLevel(Context context, int level) {
        int ops = 0;
        if (context.allocatedLayeredOCBucket.containsKey(level)) { // oc level may not be sequential
            Collection<Operation> curLayer = context.allocatedLayeredOCBucket.get(level);
            ops += curLayer.size();
            for (Operation operation : curLayer) {
                if (operation.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED)) {
                    operation.stateTransition(MetaTypes.OperationStateType.BLOCKED); // can be any state behind EXECUTED
                    notifyChildren(operation, MetaTypes.OperationStateType.BLOCKED);
                }
            }
        }
        return ops;
    }

    private void notifyChildren(Operation operation, MetaTypes.OperationStateType operationStateType) {
        for (Operation child : operation.getChildren(MetaTypes.DependencyType.TD)) {
            child.updateDependencies(MetaTypes.DependencyType.TD, operationStateType);
        }
        for (Operation child : operation.getChildren(MetaTypes.DependencyType.FD)) {
            child.updateDependencies(MetaTypes.DependencyType.FD, operationStateType);
        }
        for (Operation child : operation.getChildren(MetaTypes.DependencyType.LD)) {
            child.updateDependencies(MetaTypes.DependencyType.LD, operationStateType);
        }
    }
}