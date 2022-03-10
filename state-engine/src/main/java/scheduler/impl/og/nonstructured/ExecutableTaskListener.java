package scheduler.impl.og.nonstructured;

import scheduler.struct.og.nonstructured.AbstractNSOperationChain;
import scheduler.struct.og.nonstructured.NSOperation;

/**
 * Register an operation to queue.
 */
public class ExecutableTaskListener<ExecutionUnit extends NSOperation, SchedulingUnit extends AbstractNSOperationChain<ExecutionUnit>> {
    private final AbstractOGNSScheduler abstractOGNSScheduler;

    public ExecutableTaskListener(AbstractOGNSScheduler abstractOGNSScheduler) {
        this.abstractOGNSScheduler = abstractOGNSScheduler;
    }

    public void onOCExecutable(SchedulingUnit operationChain) {
        abstractOGNSScheduler.DISTRIBUTE(operationChain, operationChain.context);//TODO: make it clear..
    }

    public void onOCFinalized(SchedulingUnit operationChain) {
        operationChain.context.scheduledOPs += operationChain.getOperations().size();
    }

    public void onOCRollbacked(SchedulingUnit operationChain) {
        operationChain.context.scheduledOPs += operationChain.getOperations().size();
    }
}
