package scheduler.impl.og.structured;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.og.OGBFSAContext;
import scheduler.struct.op.MetaTypes;
import scheduler.struct.og.structured.bfs.BFSOperation;
import scheduler.struct.og.structured.bfs.BFSOperationChain;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static common.CONTROL.enable_log;
import static java.lang.Integer.min;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class OGBFSAScheduler extends AbstractOGBFSScheduler<OGBFSAContext> {
    private static final Logger LOG = LoggerFactory.getLogger(OGBFSAScheduler.class);
    public final ConcurrentLinkedDeque<BFSOperation> failedOperations = new ConcurrentLinkedDeque<>();//aborted operations per thread.
    public final AtomicBoolean needAbortHandling = new AtomicBoolean(false);//if any operation is aborted during processing.
    public int targetRollbackLevel = 0;//shared data structure.

    public OGBFSAScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void EXPLORE(OGBFSAContext context) {
        BFSOperationChain next = Next(context);
        if (next == null && !context.exploreFinished()) { //current level is all processed at the current thread.
            while (next == null) {
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                //all threads come to the current level.
                if (needAbortHandling.get()) {
                    if (enable_log) LOG.debug("check abort: " + context.thisThreadId + " | " + needAbortHandling.get());
                    abortHandling(context);
                }
                ProcessedToNextLevel(context);
                next = Next(context);
            }
        }
        if (context.exploreFinished()) {
            if (needAbortHandling.get()) {
                context.busyWaitQueue.clear();
                if (enable_log) LOG.debug("aborted after all ocs explored: " + context.thisThreadId + " | " + needAbortHandling.get());
                abortHandling(context);
                ProcessedToNextLevel(context);
                next = Next(context);
            }
        }
        DISTRIBUTE(next, context);
    }

//    /**
//     * Used by OGBFSScheduler.
//     *
//     * @param context
//     * @param operation_chain
//     * @param mark_ID
//     */
//    @Override
//    public void execute(OGBFSAContext context, MyList<BFSOperation> operation_chain, long mark_ID) {
//        for (BFSOperation operation : operation_chain) {
////            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
//            execute(operation, mark_ID, false);
//            checkTransactionAbort(operation, operation_chain);
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
//    public boolean executeWithBusyWait(OGBFSAContext context, BFSOperationChain operationChain, long mark_ID) {
//        MyList<BFSOperation> operation_chain_list = operationChain.getOperations();
//        for (BFSOperation operation : operation_chain_list) {
////            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
//            if (operation.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED)) continue;
//            if (isConflicted(context, operationChain, operation)) return false; // did not completed
//            execute(operation, mark_ID, false);
//            if (!operation.isFailed) {
//                operation.stateTransition(MetaTypes.OperationStateType.EXECUTED);
//            }
//            checkTransactionAbort(operation);
////            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
//        }
//        return true;
//    }

    @Override
    protected void NOTIFY(BFSOperationChain operationChain, OGBFSAContext context) {
    }

    @Override
    public void TxnSubmitFinished(OGBFSAContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<BFSOperation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            constructOp(operationGraph, request);
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    private void constructOp(List<BFSOperation> operationGraph, Request request) {
        long bid = request.txn_context.getBID();
        BFSOperation set_op;
        OGBFSAContext targetContext = getTargetContext(request.src_key);
        switch (request.accessType) {
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new BFSOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.function, request.condition, request.condition_records, request.success);
                break;
            case READ_WRITE_COND_READ:
            case READ_WRITE_COND_READN:
                set_op = new BFSOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                break;
            case READ_WRITE_READ:
                set_op = new BFSOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.record_ref, request.function);
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
    protected void checkTransactionAbort(BFSOperation operation, BFSOperationChain operationChain) {
        if (operation.isFailed
                && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            needAbortHandling.compareAndSet(false, true);
            failedOperations.push(operation); // operation need to wait until the last abort has completed
        }
    }

    protected void abortHandling(OGBFSAContext context) {
        MarkOperationsToAbort(context);

        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        IdentifyRollbackLevel(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        SetRollbackLevel(context);

        RollbackToCorrectLayerForRedo(context);
        ResumeExecution(context);
    }

    //TODO: mark operations of aborted transaction to be aborted.
    protected void MarkOperationsToAbort(OGBFSAContext context) {
        boolean markAny = false;
        ArrayList<BFSOperationChain> operationChains;
        int curLevel;
        for (Map.Entry<Integer, ArrayList<BFSOperationChain>> operationChainsEntry : context.allocatedLayeredOCBucket.entrySet()) {
            operationChains = operationChainsEntry.getValue();
            curLevel = operationChainsEntry.getKey();
            for (BFSOperationChain operationChain : operationChains) {
                for (BFSOperation operation : operationChain.getOperations()) {
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
    private boolean _MarkOperationsToAbort(OGBFSAContext context, BFSOperation operation) {
        long bid = operation.bid;
        boolean markAny = false;
        //identify bids to be aborted.
        for (BFSOperation failedOp : failedOperations) {
            if (bid == failedOp.bid) {
//                operation.aborted = true;
                operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
                markAny = true;
            }
        }
        return markAny;
    }

    protected void IdentifyRollbackLevel(OGBFSAContext context) {
        if (context.thisThreadId == 0) {
            targetRollbackLevel = Integer.MAX_VALUE;
            for (int i = 0; i < tpg.totalThreads; i++) { // find the first level that contains aborted operations
                targetRollbackLevel = min(targetRollbackLevel, tpg.threadToContextMap.get(i).rollbackLevel);
            }
        }
    }

    protected void SetRollbackLevel(OGBFSAContext context) {
        if (enable_log) LOG.debug("++++++ rollback at: " + targetRollbackLevel);
        context.rollbackLevel = targetRollbackLevel;
    }

    protected void ResumeExecution(OGBFSAContext context) {
        context.rollbackLevel = -1;
        context.isRollbacked = false;
//        if (context.thisThreadId == 0) { // TODO: what should we do to optimize this part?
        if (needAbortHandling.compareAndSet(true, false)) {
            failedOperations.clear();
        }
//        }
        if (enable_log) LOG.debug("+++++++ rollback completed...");
    }

    protected void RollbackToCorrectLayerForRedo(OGBFSAContext context) {
        int level;
        for (level = context.rollbackLevel; level <= context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        // it needs to rollback to the level -1, because aborthandling has immediately followed up with ProcessedToNextLevel
        context.currentLevel = context.rollbackLevel - 1;
    }

    protected int getNumOPsByLevel(OGBFSAContext context, int level) {
        int ops = 0;
        if (context.allocatedLayeredOCBucket.containsKey(level)) { // oc level may not be sequential
            for (BFSOperationChain operationChain : context.allocatedLayeredOCBucket.get(level)) {
                ops += operationChain.getOperations().size();
            }
        }
        return ops;
    }
}