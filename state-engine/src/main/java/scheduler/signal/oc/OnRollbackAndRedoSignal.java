package scheduler.signal.oc;

import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;

public class OnRollbackAndRedoSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        extends OperationChainSignal<OP, OC> {
    public OnRollbackAndRedoSignal(OC targetOperationChain) {
        super(targetOperationChain);
    }
}
