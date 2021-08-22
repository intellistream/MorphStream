package scheduler.signal.oc;

import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;

public class OnRootSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        extends OperationChainSignal<OP, OC> {
    public OnRootSignal(OC targetOperationChain) {
        super(targetOperationChain);
    }
}
