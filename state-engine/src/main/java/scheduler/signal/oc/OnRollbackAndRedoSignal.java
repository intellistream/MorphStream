package scheduler.signal.oc;

import scheduler.struct.og.Operation;
import scheduler.struct.og.OperationChain;

public class OnRollbackAndRedoSignal extends OperationChainSignal {
    public OnRollbackAndRedoSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
