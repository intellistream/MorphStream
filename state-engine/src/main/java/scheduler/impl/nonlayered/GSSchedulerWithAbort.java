package scheduler.impl.nonlayered;

import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.GSTPGContextWithAbort;
import scheduler.oplevel.struct.MetaTypes;
import scheduler.struct.gs.GSOperationChainWithAbort;
import scheduler.struct.gs.GSOperationWithAbort;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class GSSchedulerWithAbort extends AbstractGSScheduler<GSTPGContextWithAbort, GSOperationWithAbort, GSOperationChainWithAbort> {

    public final ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public GSSchedulerWithAbort(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(GSTPGContextWithAbort context) {
//        tpg.constructTPG(context);
        tpg.firstTimeExploreTPG(context);
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
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
    public void EXPLORE(GSTPGContextWithAbort context) {
        context.partitionStateManager.handleStateTransitions();
    }

    @Override
    public void TxnSubmitFinished(GSTPGContextWithAbort context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<GSOperationWithAbort> operationGraph = new ArrayList<>();
        int txnOpId = 0;
        GSOperationWithAbort headerOperation = null;
        GSOperationWithAbort set_op;
        for (Request request : context.requests) {
            set_op = constructOp(operationGraph, request);
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


    private GSOperationWithAbort constructOp(List<GSOperationWithAbort> operationGraph, Request request) {
        long bid = request.txn_context.getBID();
        GSOperationWithAbort set_op;
        GSTPGContextWithAbort targetContext = getTargetContext(request.src_key);
        switch (request.accessType) {
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new GSOperationWithAbort(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.function, request.condition, request.condition_records, request.success);
                break;
            case READ_WRITE_COND_READ:
                set_op = new GSOperationWithAbort(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                break;
            default:
                throw new RuntimeException("Unexpected operation");
        }
        operationGraph.add(set_op);
//        set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
//        tpg.cacheToSortedOperations(set_op);
        tpg.setupOperationTDFD(set_op, request, targetContext);
        return set_op;
    }

    @Override
    protected void NOTIFY(GSOperationChainWithAbort task, GSTPGContextWithAbort context) {
        context.partitionStateManager.onOcExecuted(task);
    }

//    @Override
//    public void PROCESS(GSTPGContextWithAbort context, long mark_ID) {
//        int threadId = context.thisThreadId;
//        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);
//        GSOperationChainWithAbort next = next(context);
//        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
//
//        if (next != null) {
////            assert !next.getOperations().isEmpty();
//            if (executeWithBusyWait(context, next, mark_ID)) { // only when executed, the notification will start.
//                MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
//                NOTIFY(next, context);
//                MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
//            }
//        } else {
//            MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);
//            next = nextFromBusyWaitQueue(context);
//            MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
//            if (next != null) {
////                assert !next.getOperations().isEmpty();
//                if (executeWithBusyWait(context, next, mark_ID)) { // only when executed, the notification will start.
//                    MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
//                    NOTIFY(next, context);
//                    MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
//                }
//            }
//        }
//    }

    /**
     * Used by GSScheduler.
     *  @param context
     * @param operationChain
     * @param mark_ID
     * @return
     */
    @Override
    public boolean execute(GSTPGContextWithAbort context, GSOperationChainWithAbort operationChain, long mark_ID) {
        MyList<GSOperationWithAbort> operation_chain_list = operationChain.getOperations();
        for (GSOperationWithAbort operation : operation_chain_list) {
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
            checkTransactionAbort(operation, operationChain);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
        }
        return true;
    }

//    /**
//     * Used by GSScheduler.
//     *  @param context
//     * @param operationChain
//     * @param mark_ID
//     * @return
//     */
//    @Override
//    public boolean executeWithBusyWait(GSTPGContextWithAbort context, GSOperationChainWithAbort operationChain, long mark_ID) {
//        MyList<GSOperationWithAbort> operation_chain_list = operationChain.getOperations();
//        assert !operationChain.isExecuted;
//        for (GSOperationWithAbort operation : operation_chain_list) {
//            if (operation.isExecuted || operation.aborted) continue;
//            if (isConflicted(context, operationChain, operation)) return false; // did not completed
//            execute(operation, mark_ID, false);
//            checkTransactionAbort(operation, operationChain);
//        }
//        return true;
//    }

    @Override
    protected void checkTransactionAbort(GSOperationWithAbort operation, GSOperationChainWithAbort operationChain) {
        if (operation.isFailed && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            operationChain.needAbortHandling = true;
            operationChain.failedOperations.add(operation);
        }
    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onOCExecutable(GSOperationChainWithAbort operationChain) {
            DISTRIBUTE(operationChain, (GSTPGContextWithAbort) operationChain.context);
        }

        public void onOCFinalized(GSOperationChainWithAbort operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
        }

        public void onOCRollbacked(GSOperationChainWithAbort operationChain) {
            operationChain.context.scheduledOPs -= operationChain.getOperations().size();
        }
    }
}
