package intellistream.morphstream.engine.txn.scheduler.signal.oc;

import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;

public class OnExecutedSignal
        extends OperationChainSignal {
    public OnExecutedSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
