package intellistream.morphstream.engine.txn.scheduler.signal.op;

import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;

public class OnProcessedSignal extends OperationSignal {
    private final boolean isFailed;

    public OnProcessedSignal(Operation operation, boolean isFailed) {
        super(operation);
        this.isFailed = isFailed;
    }

    public boolean isFailed() {
        return isFailed;
    }
}
