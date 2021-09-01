package scheduler.signal.oc;

import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;

public class OnExecutedSignal<ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>>
        extends OperationChainSignal<ExecutionUnit, SchedulingUnit> {
    public OnExecutedSignal(SchedulingUnit targetOperationChain) {
        super(targetOperationChain);
    }
}
