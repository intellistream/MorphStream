package transaction.scheduler.tpg.signal.op;

import transaction.scheduler.tpg.struct.Operation;

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
