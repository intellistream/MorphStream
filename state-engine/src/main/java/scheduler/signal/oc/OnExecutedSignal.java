package scheduler.signal.oc;

import scheduler.struct.og.OperationChain;

public class OnExecutedSignal
        extends OperationChainSignal {
    public OnExecutedSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
