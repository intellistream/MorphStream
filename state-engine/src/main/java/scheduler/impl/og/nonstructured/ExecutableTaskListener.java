package scheduler.impl.og.nonstructured;


import scheduler.context.og.AbstractOGNSContext;
import scheduler.struct.og.OperationChain;

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
