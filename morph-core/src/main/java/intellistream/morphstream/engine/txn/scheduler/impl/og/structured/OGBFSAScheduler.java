package intellistream.morphstream.engine.txn.scheduler.impl.og.structured;

import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGSAContext;
import intellistream.morphstream.engine.txn.scheduler.struct.og.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;
import intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_path;
import static java.lang.Integer.min;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class OGBFSAScheduler extends AbstractOGBFSScheduler<OGSAContext> {
    private static final Logger LOG = LoggerFactory.getLogger(OGBFSAScheduler.class);
    public final ConcurrentLinkedDeque<Operation> failedOperations = new ConcurrentLinkedDeque<>();//aborted operations per thread.
    public final AtomicBoolean needAbortHandling = new AtomicBoolean(false);//if any operation is aborted during processing.
    public int targetRollbackLevel = 0;//shared data structure.

    public OGBFSAScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void EXPLORE(OGSAContext context) {
        OperationChain next = Next(context);
        while (next == null && !context.exploreFinished()) {
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            //all threads come to the current level.
            if (needAbortHandling.get()) {
                if (enable_log) LOG.debug("check abort: " + context.thisThreadId + " | " + needAbortHandling.get());
                abortHandling(context);
            }
            ProcessedToNextLevel(context);
            // !!! We have to set a necessary barrier to avoid inconsistent abort handling.
            // e.g. T1      T2      T3      T4      [lv = 5]
            //             check   check   check
            //             lv+1    lv+1    lv+1
            //             [Process in T2/3/4 failed, need abort handling!]
            //     T1 check and proceed to abort handling at lv 5, however, the abort happend at lv6!!
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            next = Next(context);
        }
//        }
        if (context.exploreFinished()) {
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            if (needAbortHandling.get()) {
                context.busyWaitQueue.clear();
                if (enable_log)
                    LOG.debug("aborted after all ocs explored: " + context.thisThreadId + " | " + needAbortHandling.get());
                abortHandling(context);
                ProcessedToNextLevel(context);
                SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
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
//    public void execute(OGSAContext context, MyList<Operation> operation_chain, long mark_ID) {
//        for (Operation operation : operation_chain) {
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
//    public boolean executeWithBusyWait(OGSAContext context, OperationChain operationChain, long mark_ID) {
//        MyList<Operation> operation_chain_list = operationChain.getOperations();
//        for (Operation operation : operation_chain_list) {
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
    protected void NOTIFY(OperationChain operationChain, OGSAContext context) {
    }

    @Override
    protected void checkTransactionAbort(Operation operation, OperationChain operationChain) {
        if (operation.isFailed.get()
                && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            needAbortHandling.compareAndSet(false, true);
            failedOperations.push(operation); // operation need to wait until the last abort has completed
        }
    }

    protected void abortHandling(OGSAContext context) {
        MarkOperationsToAbort(context);

        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        IdentifyRollbackLevel(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        SetRollbackLevel(context);

        RollbackToCorrectLayerForRedo(context);
        ResumeExecution(context);
    }

    //TODO: mark operations of aborted transaction to be aborted.
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

    protected void IdentifyRollbackLevel(OGSAContext context) {
        if (context.thisThreadId == 0) {
            targetRollbackLevel = Integer.MAX_VALUE;
            for (int i = 0; i < tpg.totalThreads; i++) { // find the first level that contains aborted operations
                targetRollbackLevel = min(targetRollbackLevel, tpg.threadToContextMap.get(i).rollbackLevel);
            }
        }
    }

    protected void SetRollbackLevel(OGSAContext context) {
        if (enable_log) LOG.debug("++++++ rollback at: " + targetRollbackLevel);
        context.rollbackLevel = targetRollbackLevel;
    }

    protected void ResumeExecution(OGSAContext context) {
        context.rollbackLevel = -1;
        context.isRollbacked = false;
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        if (needAbortHandling.compareAndSet(true, false)) {
            failedOperations.clear();
        }
        if (enable_log) LOG.debug("+++++++ rollback completed...");
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
            }
        }
        return ops;
    }
}