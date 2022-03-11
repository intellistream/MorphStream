package scheduler.impl.og.nonstructured;

import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.og.OGNSAContext;
import scheduler.struct.op.MetaTypes;
import scheduler.struct.og.nonstructured.NSAOperationChain;
import scheduler.struct.og.nonstructured.NSAOperation;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.List;

public class OGNSAScheduler extends AbstractOGNSScheduler<OGNSAContext, NSAOperation, NSAOperationChain> {

    public final ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public OGNSAScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void INITIALIZE(OGNSAContext context) {
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
    public void EXPLORE(OGNSAContext context) {
        context.partitionStateManager.handleStateTransitions();
    }

    @Override
    public void TxnSubmitFinished(OGNSAContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<NSAOperation> operationGraph = new ArrayList<>();
        int txnOpId = 0;
        NSAOperation headerOperation = null;
        NSAOperation set_op;
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


    private NSAOperation constructOp(List<NSAOperation> operationGraph, Request request) {
        long bid = request.txn_context.getBID();
        NSAOperation set_op;
        OGNSAContext targetContext = getTargetContext(request.src_key);
        switch (request.accessType) {
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new NSAOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.function, request.condition, request.condition_records, request.success);
                break;
            case READ_WRITE_COND_READ:
            case READ_WRITE_COND_READN:
                set_op = new NSAOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
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
    protected void NOTIFY(NSAOperationChain task, OGNSAContext context) {
        context.partitionStateManager.onOcExecuted(task);
    }

    /**
     * Used by OGNSScheduler.
     *  @param context
     * @param operationChain
     * @param mark_ID
     * @return
     */
    @Override
    public boolean execute(OGNSAContext context, NSAOperationChain operationChain, long mark_ID) {
        MyList<NSAOperation> operation_chain_list = operationChain.getOperations();
        for (NSAOperation operation : operation_chain_list) {
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
            checkTransactionAbort(operation, operationChain);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
        }
        return true;
    }

    @Override
    protected void checkTransactionAbort(NSAOperation operation, NSAOperationChain operationChain) {
        if (operation.isFailed && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            operationChain.needAbortHandling = true;
            operationChain.failedOperations.add(operation);
        }
    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onOCExecutable(NSAOperationChain operationChain) {
            DISTRIBUTE(operationChain, (OGNSAContext) operationChain.context);
        }

        public void onOCFinalized(NSAOperationChain operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
        }

        public void onOCRollbacked(NSAOperationChain operationChain) {
            operationChain.context.scheduledOPs -= operationChain.getOperations().size();
        }
    }
}
