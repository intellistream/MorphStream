package scheduler.signal.op;

import scheduler.signal.NotificationSignal;
import scheduler.struct.Operation;

public abstract class OperationSignal implements NotificationSignal {
    private final Operation targetOperation;

    public OperationSignal(Operation targetOperation) {
        this.targetOperation = targetOperation;
    }

    public Operation getTargetOperation() {
        return targetOperation;
    }
}
