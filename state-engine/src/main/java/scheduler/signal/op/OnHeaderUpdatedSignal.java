package scheduler.signal.op;

import scheduler.struct.MetaTypes;
import scheduler.struct.bfs.BFSOperation;

public class OnHeaderUpdatedSignal extends OperationSignal {
    private final MetaTypes.OperationStateType headerState;

    public OnHeaderUpdatedSignal(BFSOperation operation, MetaTypes.OperationStateType headerState) {
        super(operation);
        this.headerState = headerState;
    }

    public MetaTypes.OperationStateType getState() {
        return headerState;
    }
}
