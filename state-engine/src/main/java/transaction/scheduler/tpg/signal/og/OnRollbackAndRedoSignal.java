package transaction.scheduler.tpg.signal.og;

import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationGroup;

public class OnRollbackAndRedoSignal extends OperationGroupSignal {
    private final Operation abortedOperation;
    public OnRollbackAndRedoSignal(OperationGroup targetOperationGroup, Operation abortedOperation) {
        super(targetOperationGroup);
        this.abortedOperation = abortedOperation;
    }

    public Operation getAbortedOperation() {
        return abortedOperation;
    }
}

