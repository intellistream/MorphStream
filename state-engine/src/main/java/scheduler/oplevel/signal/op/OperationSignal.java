package scheduler.oplevel.signal.op;

import scheduler.oplevel.signal.NotificationSignal;
import scheduler.oplevel.struct.Operation;

public abstract class OperationSignal implements NotificationSignal {
    private final Operation targetOperation;

    public OperationSignal(Operation targetOperation) {
        this.targetOperation = targetOperation;
    }

    public Operation getTargetOperation() {
        return targetOperation;
    }
}
