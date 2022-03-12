package scheduler.signal.oc;

import scheduler.struct.og.Operation;
import scheduler.struct.og.OperationChain;

public class OnNeedAbortHandlingSignal extends OperationChainSignal {
    private final Operation abortedOp;

    public OnNeedAbortHandlingSignal(OperationChain targetOperationChain, Operation abortedOp) {
        super(targetOperationChain);
        this.abortedOp = abortedOp;
    }

    public Operation getOperation() {
        return abortedOp;
    }
}
