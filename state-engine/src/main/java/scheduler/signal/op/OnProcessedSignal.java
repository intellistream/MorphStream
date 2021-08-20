package scheduler.signal.op;

import scheduler.struct.bfs.BFSOperation;

public class OnProcessedSignal extends OperationSignal {
    private final boolean isFailed;

    public OnProcessedSignal(BFSOperation operation, boolean isFailed) {
        super(operation);
        this.isFailed = isFailed;
    }

    public boolean isFailed() {
        return isFailed;
    }
}
