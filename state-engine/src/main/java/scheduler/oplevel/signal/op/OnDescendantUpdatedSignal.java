package scheduler.oplevel.signal.op;

import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;

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
