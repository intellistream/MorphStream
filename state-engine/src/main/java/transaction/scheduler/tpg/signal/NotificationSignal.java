package transaction.scheduler.tpg.signal;

import transaction.scheduler.tpg.struct.Operation;

public abstract class NotificationSignal {
    private final Operation targetOperation;

    public NotificationSignal(Operation targetOperation) {
        this.targetOperation = targetOperation;
    }

    public Operation getTargetOperation() {
        return targetOperation;
    }
}
