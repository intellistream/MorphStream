package scheduler.signal.oc;

import scheduler.struct.og.AbstractOperation;
import scheduler.struct.og.OperationChain;

public class OnRollbackAndRedoSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        extends OperationChainSignal<OP, OC> {
    public OnRollbackAndRedoSignal(OC targetOperationChain) {
        super(targetOperationChain);
    }
}
