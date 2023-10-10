package intellistream.morphstream.engine.txn.scheduler.signal.oc;

import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;

public class OnRollbackAndRedoSignal extends OperationChainSignal {
    public OnRollbackAndRedoSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
