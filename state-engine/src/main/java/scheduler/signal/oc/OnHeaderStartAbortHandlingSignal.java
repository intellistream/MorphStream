package scheduler.signal.oc;

import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;

public class OnHeaderStartAbortHandlingSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        extends OperationChainSignal<OP, OC> {
    private final OP abortedOp;

    public OnHeaderStartAbortHandlingSignal(OC targetOperationChain, OP abortedOp) {
        super(targetOperationChain);
        this.abortedOp = abortedOp;
    }

    public OP getOperation() {
        return abortedOp;
    }
}
