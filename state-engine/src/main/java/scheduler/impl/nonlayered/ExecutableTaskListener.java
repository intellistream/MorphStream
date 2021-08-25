package scheduler.impl.nonlayered;

import scheduler.struct.gs.AbstractGSOperationChain;
import scheduler.struct.gs.GSOperation;

/**
 * Register an operation to queue.
 */
public class ExecutableTaskListener<ExecutionUnit extends GSOperation, SchedulingUnit extends AbstractGSOperationChain<ExecutionUnit>> {
    private final AbstractGSScheduler abstractGSScheduler;

    public ExecutableTaskListener(AbstractGSScheduler abstractGSScheduler) {
        this.abstractGSScheduler = abstractGSScheduler;
    }

    public void onOCExecutable(SchedulingUnit operationChain) {
        abstractGSScheduler.DISTRIBUTE(operationChain, operationChain.context);//TODO: make it clear..
    }

    public void onOCFinalized(SchedulingUnit operationChain) {
        operationChain.context.scheduledOPs += operationChain.getOperations().size();
    }

    public void onOCRollbacked(SchedulingUnit operationChain) {
        operationChain.context.scheduledOPs += operationChain.getOperations().size();
    }
}
