package scheduler.impl.layered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.BFSLayeredTPGContext;
import scheduler.struct.bfs.BFSOperation;
import scheduler.struct.bfs.BFSOperationChain;
import scheduler.struct.dfs.DFSOperation;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.List;

import static common.CONTROL.enable_log;
import static java.lang.Integer.min;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class BFSScheduler extends LayeredScheduler<BFSLayeredTPGContext, BFSOperation, BFSOperationChain> {
    private static final Logger LOG = LoggerFactory.getLogger(BFSScheduler.class);
    public int targetRollbackLevel = 0;//shared data structure.

    public BFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    private void ProcessedToNextLevel(BFSLayeredTPGContext context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
    }

    @Override
    public void EXPLORE(BFSLayeredTPGContext context) {
        BFSOperationChain next = Next(context);
        if (next == null && !context.finished()) { //current level is all processed at the current thread.
            while (next == null) {
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                //all threads come to the current level.
                if (needAbortHandling.get()) {
                    if(enable_log) LOG.debug("check abort: " + context.thisThreadId + " | " + needAbortHandling.get());
                    abortHandling(context);
                }
                ProcessedToNextLevel(context);
                next = Next(context);
            }
        }
        DISTRIBUTE(next, context);
    }

    @Override
    protected void NOTIFY(BFSOperationChain operationChain, BFSLayeredTPGContext context) {

    }

    @Override
    public void TxnSubmitFinished(BFSLayeredTPGContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<BFSOperation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            BFSOperation set_op = null;
            switch (request.accessType) {
                case READ_WRITE_COND: // they can use the same method for processing
                case READ_WRITE:
                    set_op = new BFSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                    set_op = new BFSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                    break;
            }
            operationGraph.add(set_op);
            tpg.setupOperationTDFD(set_op, request, context);
        }

        // 4. send operation graph to tpg for tpg construction
//        tpg.setupOperationLD(operationGraph);//TODO: this is bad refactor.
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    @Override
    protected void checkTransactionAbort(BFSOperation operation) {
        if (operation.isFailed && !operation.aborted) {
            needAbortHandling.compareAndSet(false,true);
            failedOperations.push(operation); // operation need to wait until the last abort has completed
        }
    }

    @Override
    protected void abortHandling(BFSLayeredTPGContext context) {
        MarkOperationsToAbort(context);

        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        IdentifyRollbackLevel(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        SetRollbackLevel(context);

        RollbackToCorrectLayerForRedo(context);
        ResumeExecution(context);
    }

    protected void IdentifyRollbackLevel(BFSLayeredTPGContext context) {
        if (context.thisThreadId == 0) {
            targetRollbackLevel = Integer.MAX_VALUE;
            for (int i = 0; i < context.totalThreads; i++) { // find the first level that contains aborted operations
                if (enable_log) LOG.debug("is thread rollbacked: " + threadToContextMap.get(i).thisThreadId + " | " + threadToContextMap.get(i).isRollbacked);
                targetRollbackLevel = min(targetRollbackLevel, threadToContextMap.get(i).rollbackLevel);
            }
        }
    }

    protected void SetRollbackLevel(BFSLayeredTPGContext context) {
        if (enable_log) LOG.debug("++++++ rollback at: " + targetRollbackLevel);
        context.rollbackLevel = targetRollbackLevel;
    }

    protected void ResumeExecution(BFSLayeredTPGContext context) {
        context.rollbackLevel = -1;
        context.isRollbacked = false;
//        if (context.thisThreadId == 0) { // TODO: what should we do to optimize this part?
        if (needAbortHandling.compareAndSet(true, false)) {
            failedOperations.clear();
        }
//        }
    }

    protected void RollbackToCorrectLayerForRedo(BFSLayeredTPGContext context) {
        int level;
        for (level = context.rollbackLevel; level <= context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        // it needs to rollback to the level -1, because aborthandling has immediately followed up with ProcessedToNextLevel
        context.currentLevel = context.rollbackLevel-1;
    }

    protected int getNumOPsByLevel(BFSLayeredTPGContext context, int level) {
        int ops = 0;
        if (context.allocatedLayeredOCBucket.containsKey(level)) { // oc level may not be sequential
            for (BFSOperationChain operationChain : context.allocatedLayeredOCBucket.get(level)) {
                ops += operationChain.getOperations().size();
            }
        }
        return ops;
    }
}