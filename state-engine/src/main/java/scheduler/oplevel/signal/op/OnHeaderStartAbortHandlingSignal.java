package scheduler.oplevel.signal.op;

import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;

public class OnHeaderStartAbortHandlingSignal extends OperationSignal {
    private final MetaTypes.OperationStateType descendantState;

    public OnHeaderStartAbortHandlingSignal(Operation operation, MetaTypes.OperationStateType descendantState) {
        super(operation);
        this.descendantState = descendantState;
    }

    public MetaTypes.OperationStateType getState() {
        return descendantState;
    }
}
