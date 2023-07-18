package engine.txn.scheduler.signal.oc;

import engine.txn.scheduler.struct.og.Operation;
import engine.txn.scheduler.struct.og.OperationChain;

public class OnHeaderStartAbortHandlingSignal extends OperationChainSignal {
    private final Operation abortedOp;

    public OnHeaderStartAbortHandlingSignal(OperationChain targetOperationChain, Operation abortedOp) {
        super(targetOperationChain);
        this.abortedOp = abortedOp;
    }

    public Operation getOperation() {
        return abortedOp;
    }
}
