package scheduler.impl.nonlayered;

import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.GSTPGContextWithAbort;
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
        tpg.constructTPG(context);
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
        boolean existsStateTransition = context.partitionStateManager.handleStateTransitions();
//        if (!existsStateTransition) { // circular exists in the constrcuted TPG
//            if (!context.IsolatedOC.isEmpty() || !context.OCwithChildren.isEmpty()) {
//                return;
//            }
//            Collection<GSTPGContextWithAbort> contexts = tpg.getContexts();
//            if (isBlocked(contexts) && context.busyWaitQueue.isEmpty()) {
//                System.out.println("blocked and schedule all oc by busy wait");
////                GSOperationChainWithAbort oc = tpg.forceExecuteBlockedOC(context);
////                GSOperationChainWithAbort oc = forceExecuteBlockedOC(context);
////                if (oc != null) {
////                    executableTaskListener.onOCExecutable(oc);
////                }
//                for (GSOperationChainWithAbort oc : context.operationChainsLeft) {
//                    if (!oc.isExecuted) {
////                        executableTaskListener.onOCExecutable(oc);
//                        context.busyWaitQueue.add(oc);
//                    }
//                }
//            }
//        }
    }

//    public GSOperationChainWithAbort forceExecuteBlockedOC(GSTPGContextWithAbort context) {
//        return context.operationChainsLeft.poll();
//    }

    private boolean isBlocked(Collection<GSTPGContextWithAbort> contexts) {
        boolean isBlocked = true;
        for (GSTPGContextWithAbort context : contexts) {
            if (!context.finished()) {
                if (!context.IsolatedOC.isEmpty() || !context.OCwithChildren.isEmpty() || !context.partitionStateManager.ocSignalQueue.isEmpty()) {
                    isBlocked = false;
                    break;
                }
            }
        }
        return isBlocked;
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
        switch (request.accessType) {
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new GSOperationWithAbort(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.function, request.condition, request.condition_records, request.success);
                break;
            case READ_WRITE_COND_READ:
                set_op = new GSOperationWithAbort(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                break;
            default:
                throw new RuntimeException("Unexpected operation");
        }
        operationGraph.add(set_op);
        set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
        tpg.cacheToSortedOperations(set_op);
//        set_op.setOC(tpg.setupOperationTDFD(set_op, request));
        return set_op;
    }

    @Override
    protected void NOTIFY(GSOperationChainWithAbort task, GSTPGContextWithAbort context) {
        context.partitionStateManager.onOcExecuted(task);
    }

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

    /**
     * Used by GSScheduler.
     *  @param context
     * @param operationChain
     * @param mark_ID
     * @return
     */
    @Override
    public boolean executeWithBusyWait(GSTPGContextWithAbort context, GSOperationChainWithAbort operationChain, long mark_ID) {
        MyList<GSOperationWithAbort> operation_chain_list = operationChain.getOperations();
        for (GSOperationWithAbort operation : operation_chain_list) {
            if (operation.isExecuted) continue;
            if (isConflicted(context, operationChain, operation)) return false; // did not completed
            System.out.println("executing: " + operation);
            execute(operation, mark_ID, false);
            checkTransactionAbort(operation, operationChain);
        }
        return true;
    }

    protected void checkTransactionAbort(GSOperationWithAbort operation, GSOperationChainWithAbort operationChain) {
        if (operation.isFailed && !operation.aborted) {
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
//            System.out.println(operationChain.context + " - " + operationChain.context.scheduledOPs);
//            operationChain.context.operaitonsLeft.removeAll(operationChain.getOperations());
//            operationChain.context.operationChainsLeft.remove(operationChain);
        }

        public void onOCRollbacked(GSOperationChainWithAbort operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
        }
    }
}
