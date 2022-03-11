package scheduler.impl.og.structured;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.og.OGDFSAContext;
import scheduler.struct.op.MetaTypes;
import scheduler.struct.og.structured.dfs.DFSOperation;
import scheduler.struct.og.structured.dfs.DFSOperationChain;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static common.CONTROL.enable_log;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class OGDFSAScheduler extends AbstractOGDFSScheduler<OGDFSAContext> {
    private static final Logger LOG = LoggerFactory.getLogger(OGDFSAScheduler.class);

    public final ConcurrentLinkedDeque<DFSOperation> failedOperations = new ConcurrentLinkedDeque<>();//aborted operations per thread.
    public final AtomicBoolean needAbortHandling = new AtomicBoolean(false);//if any operation is aborted during processing.

    public OGDFSAScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void EXPLORE(OGDFSAContext context) {
        DFSOperationChain oc = Next(context);
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
//    public void execute(OGDFSAContext context, MyList<DFSOperation> operation_chain, long mark_ID) {
//        for (DFSOperation operation : operation_chain) {
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
//    public boolean executeWithBusyWait(OGDFSAContext context, DFSOperationChain operationChain, long mark_ID) {
//        MyList<DFSOperation> operation_chain_list = operationChain.getOperations();
//        for (DFSOperation operation : operation_chain_list) {
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
    protected void NOTIFY(DFSOperationChain operationChain, OGDFSAContext context) {
//        context.partitionStateManager.onOcExecuted(operationChain);
        operationChain.isExecuted = true; // set operation chain to be executed, which is used for further rollback
        Collection<DFSOperationChain> ocs = operationChain.getChildren();
        for (DFSOperationChain childOC : ocs) {
            childOC.updateDependency();
        }
    }

    @Override
    public void TxnSubmitFinished(OGDFSAContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<DFSOperation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            constructOp(operationGraph, request);
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    private void constructOp(List<DFSOperation> operationGraph, Request request) {
        long bid = request.txn_context.getBID();
        DFSOperation set_op = null;
        OGDFSAContext targetContext = getTargetContext(request.src_key);
        switch (request.accessType) {
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new DFSOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.function, request.condition, request.condition_records, request.success);
                break;
            case READ_WRITE_COND_READ:
            case READ_WRITE_COND_READN:
                set_op = new DFSOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        operationGraph.add(set_op);
//        set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
//        tpg.cacheToSortedOperations(set_op);
        tpg.setupOperationTDFD(set_op, request, targetContext);
    }

    @Override
    protected void checkTransactionAbort(DFSOperation operation, DFSOperationChain dfsOperationChain) {
        if (operation.isFailed && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            for (DFSOperation failedOp : failedOperations) {
                if (failedOp.bid == operation.bid) {
                    return;
                }
            }
            needAbortHandling.compareAndSet(false, true);
            failedOperations.push(operation); // operation need to wait until the last abort has completed
        }
    }

    protected void abortHandling(OGDFSAContext context) {
        MarkOperationsToAbort(context);
        RollbackToCorrectLayerForRedo(context);
        ResumeExecution(context);
    }

    protected void MarkOperationsToAbort(OGDFSAContext context) {
        boolean markAny = false;
        ArrayList<DFSOperationChain> operationChains;
        int curLevel;
        for (Map.Entry<Integer, ArrayList<DFSOperationChain>> operationChainsEntry : context.allocatedLayeredOCBucket.entrySet()) {
            operationChains = operationChainsEntry.getValue();
            curLevel = operationChainsEntry.getKey();
            for (DFSOperationChain operationChain : operationChains) {
                for (DFSOperation operation : operationChain.getOperations()) {
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
    private boolean _MarkOperationsToAbort(OGDFSAContext context, DFSOperation operation) {
        long bid = operation.bid;
        boolean markAny = false;
        //identify bids to be aborted.
        for (DFSOperation failedOp : failedOperations) {
            if (bid == failedOp.bid) {
                operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
                markAny = true;
            }
        }
        return markAny;
    }

    protected void ResumeExecution(OGDFSAContext context) {
        context.rollbackLevel = -1;
        context.isRollbacked = false;

        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        needAbortHandling.compareAndSet(true, false);
        failedOperations.clear();
        LOG.debug("resume: " + context.thisThreadId);
    }

    protected void RollbackToCorrectLayerForRedo(OGDFSAContext context) {
        int level;
        for (level = context.rollbackLevel; level <= context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        // it needs to rollback to the level -1, because aborthandling has immediately followed up with ProcessedToNextLevel
        context.currentLevel = context.rollbackLevel - 1;
    }

    protected int getNumOPsByLevel(OGDFSAContext context, int level) {
        int ops = 0;
        if (context.allocatedLayeredOCBucket.containsKey(level)) { // oc level may not be sequential
            for (DFSOperationChain operationChain : context.allocatedLayeredOCBucket.get(level)) {
                ops += operationChain.getOperations().size();
                if (operationChain.isExecuted) { // rollback children counter if the parent has been rollback
                    operationChain.isExecuted = false;
                    Collection<DFSOperationChain> ocs = operationChain.getChildren();
                    for (DFSOperationChain childOC : ocs) {
                        childOC.rollbackDependency();
                    }
                }
            }
        }
        return ops;
    }
}
