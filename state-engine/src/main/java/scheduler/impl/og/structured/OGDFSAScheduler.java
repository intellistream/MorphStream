package scheduler.impl.og.structured;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.context.og.OGSAContext;
import scheduler.struct.og.Operation;
import scheduler.struct.og.OperationChain;
import scheduler.struct.op.MetaTypes;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static common.CONTROL.enable_log;
import static utils.FaultToleranceConstants.LOGOption_path;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class OGDFSAScheduler extends AbstractOGDFSScheduler<OGSAContext> {
    private static final Logger LOG = LoggerFactory.getLogger(OGDFSAScheduler.class);

    public final ConcurrentLinkedDeque<Operation> failedOperations = new ConcurrentLinkedDeque<>();//aborted operations per thread.
    public final AtomicBoolean needAbortHandling = new AtomicBoolean(false); //if any operation is aborted during processing.

    public OGDFSAScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void EXPLORE(OGSAContext context) {
        OperationChain oc = Next(context);
        while (oc == null) {
            // current thread finishes the current level
            if (needAbortHandling.get()) {
                abortHandling(context);
            }
            if (context.exploreFinished())
                break;
            ProcessedToNextLevel(context);
            oc = Next(context);
        }
        while (oc != null && oc.hasParents()) {
            // Busy-Wait for dependency resolution
            if (needAbortHandling.get()) {
                return; // skip this busy wait when abort happens
            }
        }
        DISTRIBUTE(oc, context);
    }

//    /**
//     * Used by OGBFSScheduler.
//     *
//     * @param context
//     * @param operation_chain
//     * @param mark_ID
//     */
//    @Override
//    public void execute(OGSAContext context, MyList<Operation> operation_chain, long mark_ID) {
//        for (Operation operation : operation_chain) {
////            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
//            execute(operation, mark_ID, false);
//            checkTransactionAbort(operation);
////            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
//        }
//    }

//    /**
//     * Used by OGNSScheduler.
//     *  @param context
//     * @param operationChain
//     * @param mark_ID
//     * @return
//     */
//    @Override
//    public boolean executeWithBusyWait(OGSAContext context, OperationChain operationChain, long mark_ID) {
//        MyList<Operation> operation_chain_list = operationChain.getOperations();
//        for (Operation operation : operation_chain_list) {
////            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
//            if (operation.isExecuted) continue;
//            if (isConflicted(context, operationChain, operation)) return false; // did not completed
//            execute(operation, mark_ID, false);
//            checkTransactionAbort(operation);
////            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
//        }
//        return true;
//    }

    /**
     * notify is handled by state manager of each thread
     *
     * @param operationChain
     * @param context
     */
    @Override
    protected void NOTIFY(OperationChain operationChain, OGSAContext context) {
//        context.partitionStateManager.onOcExecuted(operationChain);
        operationChain.isExecuted = true; // set operation chain to be executed, which is used for further rollback
        Collection<OperationChain> ocs = operationChain.getChildren();
        for (OperationChain childOC : ocs) {
            childOC.updateDependency();
        }
    }

    @Override
    protected void checkTransactionAbort(Operation operation, OperationChain OperationChain) {
        if (operation.isFailed && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            for (Operation failedOp : failedOperations) {
                if (failedOp.bid == operation.bid) {
                    return;
                }
            }
            needAbortHandling.compareAndSet(false, true);
            failedOperations.push(operation); // operation need to wait until the last abort has completed
        }
    }

    protected void abortHandling(OGSAContext context) {
        MarkOperationsToAbort(context);
        RollbackToCorrectLayerForRedo(context);
        ResumeExecution(context);
    }

    protected void MarkOperationsToAbort(OGSAContext context) {
        boolean markAny = false;
        ArrayList<OperationChain> operationChains;
        int curLevel;
        for (Map.Entry<Integer, ArrayList<OperationChain>> operationChainsEntry : context.allocatedLayeredOCBucket.entrySet()) {
            operationChains = operationChainsEntry.getValue();
            curLevel = operationChainsEntry.getKey();
            for (OperationChain operationChain : operationChains) {
                for (Operation operation : operationChain.getOperations()) {
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
        if (enable_log) LOG.debug("++++++ rollback at level: " + context.thisThreadId + " | " + context.rollbackLevel);
    }

    /**
     * Mark operations of an aborted transaction to abort.
     *
     * @param context
     * @param operation
     * @return
     */
    private boolean _MarkOperationsToAbort(OGSAContext context, Operation operation) {
        long bid = operation.bid;
        boolean markAny = false;
        //identify bids to be aborted.
        for (Operation failedOp : failedOperations) {
            if (bid == failedOp.bid) {
                operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
                if (this.isLogging == LOGOption_path && operation.getTxnOpId() == 0) {
                    MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(context.thisThreadId);
                    this.tpg.threadToPathRecord.get(context.thisThreadId).addAbortBid(operation.bid);
                    MeasureTools.END_SCHEDULE_TRACKING_TIME_MEASURE(context.thisThreadId);
                }
                markAny = true;
            }
        }
        return markAny;
    }

    protected void ResumeExecution(OGSAContext context) {
        context.rollbackLevel = -1;
        context.isRollbacked = false;

        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        needAbortHandling.compareAndSet(true, false);
        failedOperations.clear();
        LOG.debug("resume: " + context.thisThreadId);
    }

    protected void RollbackToCorrectLayerForRedo(OGSAContext context) {
        int level;
        for (level = context.rollbackLevel; level <= context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        // it needs to rollback to the level -1, because aborthandling has immediately followed up with ProcessedToNextLevel
        context.currentLevel = context.rollbackLevel - 1;
    }

    protected int getNumOPsByLevel(OGSAContext context, int level) {
        int ops = 0;
        if (context.allocatedLayeredOCBucket.containsKey(level)) { // oc level may not be sequential
            for (OperationChain operationChain : context.allocatedLayeredOCBucket.get(level)) {
                ops += operationChain.getOperations().size();
                if (operationChain.isExecuted) { // rollback children counter if the parent has been rollback
                    operationChain.isExecuted = false;
                    Collection<OperationChain> ocs = operationChain.getChildren();
                    for (OperationChain childOC : ocs) {
                        childOC.rollbackDependency();
                    }
                }
            }
        }
        return ops;
    }
}
