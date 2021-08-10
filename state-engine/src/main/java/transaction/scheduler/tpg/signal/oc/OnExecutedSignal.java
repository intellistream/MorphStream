package transaction.scheduler.tpg.signal.oc;

import transaction.scheduler.tpg.struct.OperationChain;

public class OnExecutedSignal extends OperationChainSignal {
    public OnExecutedSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
