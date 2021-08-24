package scheduler.impl.nonlayered;

import org.jetbrains.annotations.NotNull;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.GSTPGContext;
import scheduler.impl.Scheduler;
import scheduler.struct.bfs.BFSOperation;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChain;
import transaction.impl.ordered.MyList;

import java.util.ArrayList;
import java.util.List;

public class GSScheduler extends Scheduler<GSTPGContext, GSOperation, GSOperationChain> {

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public GSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(GSTPGContext context) {
        tpg.firstTimeExploreTPG(context);
        context.partitionStateManager.initialize(executableTaskListener);
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
    public void EXPLORE(GSTPGContext context) {
        context.partitionStateManager.handleStateTransitions();
    }

    @Override
    public boolean FINISHED(GSTPGContext context) {
        return context.finished();
    }

    @Override
    public void RESET() {
//        Controller.exec.shutdownNow();
    }

    @Override
    public void TxnSubmitFinished(GSTPGContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<GSOperation> operationGraph = new ArrayList<>();
        int txnOpId = 0;
        GSOperation headerOperation = null;
        for (Request request : context.requests) {
            GSOperation set_op = constructOp(context, operationGraph, request);
            if (txnOpId == 0)
                headerOperation = set_op;
            // addOperation an operation id for the operation for the purpose of temporal dependency construction
            set_op.setTxnOpId(txnOpId++);
            set_op.addHeader(headerOperation);
            headerOperation.addDescendant(set_op);
        }
        // set logical dependencies among all operation in the same transaction
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    @NotNull
    private GSOperation constructOp(GSTPGContext context, List<GSOperation> operationGraph, Request request) {
        long bid = request.txn_context.getBID();
        GSOperation set_op;
        switch (request.accessType) {
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new GSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.function, request.condition, request.condition_records, request.success);
                break;
            case READ_WRITE_COND_READ:
                set_op = new GSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                break;
            default:
                throw new RuntimeException("Unexpected operation");
        }
        operationGraph.add(set_op);
        set_op.setOC(tpg.setupOperationTDFD(set_op, request, context));
        return set_op;
    }

    @Override
    public void PROCESS(GSTPGContext context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);
        GSOperationChain next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        if (next != null) {
            execute(context, next, mark_ID);
            NOTIFY(next, context);
        }
    }

    @Override
    protected void NOTIFY(GSOperationChain task, GSTPGContext context) {
        context.partitionStateManager.onOcExecuted(task);
    }

    /**
     * Used by GSScheduler.
     *
     * @param context
     * @param operationChain
     * @param mark_ID
     */
    public void execute(GSTPGContext context, GSOperationChain operationChain, long mark_ID) {
        MyList<GSOperation> operation_chain_list = operationChain.getOperations();
        for (GSOperation operation : operation_chain_list) {
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
            checkTransactionAbort(operation, operationChain);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
        }
    }

    protected void checkTransactionAbort(GSOperation operation, GSOperationChain operationChain) {
        if (operation.isFailed && !operation.aborted) {
            operationChain.needAbortHandling = true;
            operationChain.failedOperations.add(operation);
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    private GSOperationChain next(GSTPGContext context) {
        GSOperationChain operationChain = context.OCwithChildren.pollLast();
        if (operationChain == null) {
            return context.IsolatedOC.pollLast();
        }
        return operationChain;
    }

    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     */
    @Override
    public void DISTRIBUTE(GSOperationChain task, GSTPGContext context) {
        if (task != null) {
            if (!task.hasChildren()) {
                context.IsolatedOC.add(task);
            } else {
                context.OCwithChildren.add(task);
            }
        }
    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onOCExecutable(GSOperationChain operationChain) {
            DISTRIBUTE(operationChain, operationChain.context);//TODO: make it clear..
        }
        public void onOCFinalized(GSOperationChain operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
        }
        public void onOCRollbacked(GSOperationChain operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
        }
    }
}
