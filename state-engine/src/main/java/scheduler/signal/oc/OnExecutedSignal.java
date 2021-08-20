package scheduler.signal.oc;

import scheduler.struct.bfs.BFSOperationChain;

public class OnExecutedSignal extends OperationChainSignal {
    public OnExecutedSignal(BFSOperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
