package engine.txn.scheduler.signal.oc;

import engine.txn.scheduler.struct.og.OperationChain;

public class OnExecutedSignal
        extends OperationChainSignal {
    public OnExecutedSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
