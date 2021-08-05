package transaction.scheduler.tpg.signal;

import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;

public class OnHeaderUpdatedSignal extends NotificationSignal {
    private final MetaTypes.OperationStateType headerState;

    public OnHeaderUpdatedSignal(Operation operation, MetaTypes.OperationStateType headerState) {
        super(operation);
        this.headerState = headerState;
    }

    public MetaTypes.OperationStateType getState() {
        return headerState;
    }
}
