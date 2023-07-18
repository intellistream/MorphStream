package engine.txn.scheduler.signal.oc;

import engine.txn.scheduler.struct.og.OperationChain;

public class OnRootSignal extends OperationChainSignal {
    public OnRootSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
