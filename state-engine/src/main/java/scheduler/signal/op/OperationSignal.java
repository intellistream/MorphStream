package scheduler.signal.op;

import scheduler.signal.NotificationSignal;
import scheduler.struct.layered.bfs.BFSOperation;

public abstract class OperationSignal implements NotificationSignal {
    private final BFSOperation targetOperation;

    public OperationSignal(BFSOperation targetOperation) {
        this.targetOperation = targetOperation;
    }

    public BFSOperation getTargetOperation() {
        return targetOperation;
    }
}
