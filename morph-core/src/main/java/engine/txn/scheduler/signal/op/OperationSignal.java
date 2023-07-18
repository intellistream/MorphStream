package engine.txn.scheduler.signal.op;

import engine.txn.scheduler.struct.op.Operation;
import engine.txn.scheduler.signal.NotificationSignal;

public abstract class OperationSignal implements NotificationSignal {
    private final Operation targetOperation;

    public OperationSignal(Operation targetOperation) {
        this.targetOperation = targetOperation;
    }

    public Operation getTargetOperation() {
        return targetOperation;
    }
}
