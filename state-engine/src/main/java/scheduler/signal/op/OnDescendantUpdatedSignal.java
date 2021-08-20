package scheduler.signal.op;

import scheduler.struct.MetaTypes;
import scheduler.struct.bfs.BFSOperation;

public class OnDescendantUpdatedSignal extends OperationSignal {
    private final MetaTypes.OperationStateType descendantState;

    public OnDescendantUpdatedSignal(BFSOperation operation, MetaTypes.OperationStateType descendantState) {
        super(operation);
        this.descendantState = descendantState;
    }

    public MetaTypes.OperationStateType getState() {
        return descendantState;
    }
}
