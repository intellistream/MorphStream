package scheduler.signal.oc;

import scheduler.struct.og.AbstractOperation;
import scheduler.struct.og.OperationChain;

public class OnExecutedSignal<ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>>
        extends OperationChainSignal<ExecutionUnit, SchedulingUnit> {
    public OnExecutedSignal(SchedulingUnit targetOperationChain) {
        super(targetOperationChain);
    }
}
