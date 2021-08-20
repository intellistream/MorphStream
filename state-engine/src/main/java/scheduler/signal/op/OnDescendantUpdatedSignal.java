package scheduler.signal.op;

import scheduler.struct.MetaTypes;
import scheduler.struct.Operation;

public class OnDescendantUpdatedSignal extends OperationSignal {
    private final MetaTypes.OperationStateType descendantState;

    public OnDescendantUpdatedSignal(Operation operation, MetaTypes.OperationStateType descendantState) {
        super(operation);
        this.descendantState = descendantState;
    }

    public MetaTypes.OperationStateType getState() {
        return descendantState;
    }
}
