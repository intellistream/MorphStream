package scheduler.signal.op;

import scheduler.struct.layered.bfs.BFSOperation;

public class OnRootSignal extends OperationSignal {
    public OnRootSignal(BFSOperation targetOperation) {
        super(targetOperation);
    }
}
