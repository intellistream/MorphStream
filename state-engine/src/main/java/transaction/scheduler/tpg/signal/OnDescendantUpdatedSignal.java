package transaction.scheduler.tpg.signal;

import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;

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
