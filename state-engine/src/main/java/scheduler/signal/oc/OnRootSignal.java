package scheduler.signal.oc;

import scheduler.struct.OperationChain;

public class OnRootSignal extends OperationChainSignal {
    public OnRootSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
