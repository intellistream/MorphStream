package scheduler.signal.oc;

import scheduler.signal.NotificationSignal;
import scheduler.struct.og.AbstractOperation;
import scheduler.struct.og.OperationChain;

public abstract class OperationChainSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        implements NotificationSignal {
    private final OC targetOperationChain;

    public OperationChainSignal(OC targetOperationChain) {
        this.targetOperationChain = targetOperationChain;
    }

    public OC getTargetOperationChain() {
        return targetOperationChain;
    }
}
