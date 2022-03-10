package scheduler.signal.oc;

import scheduler.struct.og.AbstractOperation;
import scheduler.struct.og.OperationChain;

public class OnRootSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        extends OperationChainSignal<OP, OC> {
    public OnRootSignal(OC targetOperationChain) {
        super(targetOperationChain);
    }
}
