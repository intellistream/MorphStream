package scheduler.signal.oc;

import scheduler.signal.NotificationSignal;
import scheduler.struct.og.Operation;
import scheduler.struct.og.OperationChain;

public abstract class OperationChainSignal implements NotificationSignal {
    private final OperationChain targetOperationChain;

    public OperationChainSignal(OperationChain targetOperationChain) {
        this.targetOperationChain = targetOperationChain;
    }

    public OperationChain getTargetOperationChain() {
        return targetOperationChain;
    }
}
