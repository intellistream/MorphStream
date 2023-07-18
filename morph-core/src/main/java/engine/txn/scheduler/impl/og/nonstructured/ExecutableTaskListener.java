package engine.txn.scheduler.impl.og.nonstructured;


import engine.txn.scheduler.context.og.AbstractOGNSContext;
import engine.txn.scheduler.struct.og.OperationChain;

/**
 * Register an operation to queue.
 */
public class ExecutableTaskListener {
    private final AbstractOGNSScheduler abstractOGNSScheduler;

    public ExecutableTaskListener(AbstractOGNSScheduler abstractOGNSScheduler) {
        this.abstractOGNSScheduler = abstractOGNSScheduler;
    }

    public void onOCExecutable(OperationChain operationChain) {
        abstractOGNSScheduler.DISTRIBUTE(operationChain, (AbstractOGNSContext) operationChain.context);//TODO: make it clear..
    }

    public void onOCFinalized(OperationChain operationChain) {
        operationChain.context.scheduledOPs += operationChain.getOperations().size();
    }

    public void onOCRollbacked(OperationChain operationChain) {
        operationChain.context.scheduledOPs += operationChain.getOperations().size();
    }
}
