package engine.txn.scheduler.signal.oc;

import engine.txn.scheduler.struct.og.OperationChain;
import engine.txn.scheduler.signal.NotificationSignal;

public abstract class OperationChainSignal implements NotificationSignal {
    private final OperationChain targetOperationChain;

    public OperationChainSignal(OperationChain targetOperationChain) {
        this.targetOperationChain = targetOperationChain;
    }

    public OperationChain getTargetOperationChain() {
        return targetOperationChain;
    }
}
