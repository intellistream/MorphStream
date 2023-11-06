package intellistream.morphstream.engine.txn.scheduler.signal.op;

import intellistream.morphstream.engine.txn.scheduler.signal.NotificationSignal;
import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;

public abstract class OperationSignal implements NotificationSignal {
    private final Operation targetOperation;

    public OperationSignal(Operation targetOperation) {
        this.targetOperation = targetOperation;
    }

    public Operation getTargetOperation() {
        return targetOperation;
    }
}
