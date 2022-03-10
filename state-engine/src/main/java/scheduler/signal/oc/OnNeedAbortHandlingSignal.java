package scheduler.signal.oc;

import scheduler.struct.og.AbstractOperation;
import scheduler.struct.og.OperationChain;

public class OnNeedAbortHandlingSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        extends OperationChainSignal<OP, OC> {
    private final OP abortedOp;

    public OnNeedAbortHandlingSignal(OC targetOperationChain, OP abortedOp) {
        super(targetOperationChain);
        this.abortedOp = abortedOp;
    }

    public OP getOperation() {
        return abortedOp;
    }
}
