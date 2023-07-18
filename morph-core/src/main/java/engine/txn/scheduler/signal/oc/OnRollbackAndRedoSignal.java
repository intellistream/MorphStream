package engine.txn.scheduler.signal.oc;

import engine.txn.scheduler.struct.og.OperationChain;

public class OnRollbackAndRedoSignal extends OperationChainSignal {
    public OnRollbackAndRedoSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
