package transaction.scheduler.tpg.signal;

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
