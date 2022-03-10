package scheduler.signal.op;

import scheduler.struct.op.MetaTypes;
import scheduler.struct.op.Operation;

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
