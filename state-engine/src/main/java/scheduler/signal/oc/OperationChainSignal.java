package scheduler.signal.oc;

import scheduler.signal.NotificationSignal;
import scheduler.struct.bfs.BFSOperationChain;

public abstract class OperationChainSignal implements NotificationSignal {
    private final BFSOperationChain targetOperationChain;

    public OperationChainSignal(BFSOperationChain targetOperationChain) {
        this.targetOperationChain = targetOperationChain;
    }

    public BFSOperationChain getTargetOperationChain() {
        return targetOperationChain;
    }
}
