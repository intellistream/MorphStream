package scheduler.impl.layered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.DFSLayeredTPGContext;
import scheduler.struct.dfs.DFSOperation;
import scheduler.struct.dfs.DFSOperationChain;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class DFSScheduler extends LayeredScheduler<DFSLayeredTPGContext, DFSOperation, DFSOperationChain> {
    private static final Logger LOG = LoggerFactory.getLogger(DFSScheduler.class);


    public DFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    private void ProcessedToNextLevel(DFSLayeredTPGContext context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
    }

    @Override
    public void EXPLORE(DFSLayeredTPGContext context) {
        DFSOperationChain oc = Next(context);
        while (oc == null) {
            //all threads come to the current level.
            if (needAbortHandling.get()) {
                abortHandling(context);
            }
            if (context.finished())
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

    /**
     * notify is handled by state manager of each thread
     *
     * @param operationChain
     * @param context
     */
    @Override
    protected void NOTIFY(DFSOperationChain operationChain, DFSLayeredTPGContext context) {
//        context.partitionStateManager.onOcExecuted(operationChain);
        operationChain.isExecuted = true; // set operation chain to be executed, which is used for further rollback
        Collection<DFSOperationChain> ocs = operationChain.getFDChildren();
        for (DFSOperationChain childOC : ocs) {
            childOC.updateDependency();
        }
    }

    @Override
    public void TxnSubmitFinished(DFSLayeredTPGContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<DFSOperation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            DFSOperation set_op = null;
            switch (request.accessType) {
                case READ_WRITE_COND: // they can use the same method for processing
                case READ_WRITE:
                    set_op = new DFSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                    set_op = new DFSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
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
    protected void checkTransactionAbort(DFSOperation operation) {
        if (operation.isFailed && !operation.aborted) {
            for (DFSOperation failedOp : failedOperations) {
                if (failedOp.bid == operation.bid) {
                    return;
                }
            }
            needAbortHandling.compareAndSet(false,true);
            failedOperations.push(operation); // operation need to wait until the last abort has completed
        }
    }

    @Override
    protected void abortHandling(DFSLayeredTPGContext context) {
        MarkOperationsToAbort(context);
        RollbackToCorrectLayerForRedo(context);
        ResumeExecution(context);
    }

    protected void ResumeExecution(DFSLayeredTPGContext context) {
        context.rollbackLevel = -1;
        context.isRollbacked = false;

        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        needAbortHandling.compareAndSet(true, false);
        failedOperations.clear();
        LOG.debug("resume: " + context.thisThreadId);
    }

    protected void RollbackToCorrectLayerForRedo(DFSLayeredTPGContext context) {
        int level;
        for (level = context.rollbackLevel; level <= context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        // it needs to rollback to the level -1, because aborthandling has immediately followed up with ProcessedToNextLevel
        context.currentLevel = context.rollbackLevel-1;
    }

    protected int getNumOPsByLevel(DFSLayeredTPGContext context, int level) {
        int ops = 0;
        if (context.allocatedLayeredOCBucket.containsKey(level)) { // oc level may not be sequential
            for (DFSOperationChain operationChain : context.allocatedLayeredOCBucket.get(level)) {
                ops += operationChain.getOperations().size();
                if (operationChain.isExecuted) { // rollback children counter if the parent has been rollback
                    operationChain.isExecuted = false;
                    Collection<DFSOperationChain> ocs = operationChain.getFDChildren();
                    for (DFSOperationChain childOC : ocs) {
                        childOC.rollbackDependency();
                    }
                }
            }
        }
        return ops;
    }
}
