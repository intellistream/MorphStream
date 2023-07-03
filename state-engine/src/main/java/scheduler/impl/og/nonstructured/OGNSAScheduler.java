package scheduler.impl.og.nonstructured;

import scheduler.context.og.OGNSAContext;
import scheduler.struct.og.Operation;
import scheduler.struct.og.OperationChain;
import scheduler.struct.op.MetaTypes;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;

public class OGNSAScheduler extends AbstractOGNSScheduler<OGNSAContext> {

    public final ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public OGNSAScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void INITIALIZE(OGNSAContext context) {
//        tpg.constructTPG(context);
        tpg.firstTimeExploreTPG(context);
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
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
    protected void NOTIFY(OperationChain task, OGNSAContext context) {
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
    public boolean execute(OGNSAContext context, OperationChain operationChain, long mark_ID) {
        MyList<Operation> operation_chain_list = operationChain.getOperations();
        for (Operation operation : operation_chain_list) {
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
            checkTransactionAbort(operation, operationChain);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
        }
        return true;
    }

    @Override
    protected void checkTransactionAbort(Operation operation, OperationChain operationChain) {
        if (operation.isFailed && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            operationChain.needAbortHandling = true;
            operationChain.failedOperations.add(operation);
        }
    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onOCExecutable(OperationChain operationChain) {
            DISTRIBUTE(operationChain, (OGNSAContext) operationChain.context);
        }

        public void onOCFinalized(OperationChain operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
        }

        public void onOCRollbacked(OperationChain operationChain) {
            operationChain.context.scheduledOPs -= operationChain.getOperations().size();
        }
    }
}
