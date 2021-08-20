package scheduler.signal.oc;

import scheduler.struct.bfs.BFSOperationChain;

public class OnRootSignal extends OperationChainSignal {
    public OnRootSignal(BFSOperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
