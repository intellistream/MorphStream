package intellistream.morphstream.engine.txn.scheduler.signal.oc;

import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;

public class OnRootSignal extends OperationChainSignal {
    public OnRootSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
