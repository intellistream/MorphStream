package transaction.scheduler.tpg.signal;

import transaction.scheduler.tpg.struct.Operation;

public abstract class OperationSignal implements NotificationSignal {
    private final Operation targetOperation;

    public OperationSignal(Operation targetOperation) {
        this.targetOperation = targetOperation;
    }

    public Operation getTargetOperation() {
        return targetOperation;
    }
}
