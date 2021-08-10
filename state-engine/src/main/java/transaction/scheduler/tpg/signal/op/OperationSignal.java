package transaction.scheduler.tpg.signal.op;

import transaction.scheduler.tpg.signal.NotificationSignal;
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
