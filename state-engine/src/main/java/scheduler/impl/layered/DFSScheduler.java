package scheduler.impl.layered;

import org.jetbrains.annotations.Nullable;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.DFSLayeredTPGContext;
import scheduler.struct.layered.dfs.DFSOperation;
import scheduler.struct.layered.dfs.DFSOperationChain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class DFSScheduler extends AbstractDFSScheduler<DFSLayeredTPGContext> {


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
            if (context.finished())
                break;
            ProcessedToNextLevel(context);
            oc = Next(context);
        }
        while (oc != null && oc.hasParents());
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
            DFSOperation set_op = constructOp(operationGraph, request);
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    @Nullable
    private DFSOperation constructOp(List<DFSOperation> operationGraph, Request request) {
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
        tpg.setupOperationTDFD(set_op, request);
        return set_op;
    }
}
