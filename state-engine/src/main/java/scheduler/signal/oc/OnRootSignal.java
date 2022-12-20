package scheduler.signal.oc;

import scheduler.struct.og.OperationChain;

public class OnRootSignal extends OperationChainSignal {
    public OnRootSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
