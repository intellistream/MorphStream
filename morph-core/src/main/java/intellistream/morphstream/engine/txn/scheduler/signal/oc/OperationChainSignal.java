package intellistream.morphstream.engine.txn.scheduler.signal.oc;

import intellistream.morphstream.engine.txn.scheduler.signal.NotificationSignal;
import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;

public abstract class OperationChainSignal implements NotificationSignal {
    private final OperationChain targetOperationChain;

    public OperationChainSignal(OperationChain targetOperationChain) {
        this.targetOperationChain = targetOperationChain;
    }

    public OperationChain getTargetOperationChain() {
        return targetOperationChain;
    }
}
