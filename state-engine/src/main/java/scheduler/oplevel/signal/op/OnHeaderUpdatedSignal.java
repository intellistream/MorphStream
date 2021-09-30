package scheduler.oplevel.signal.op;

import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;

public class OnHeaderUpdatedSignal extends OperationSignal {
    private final MetaTypes.OperationStateType headerState;

    public OnHeaderUpdatedSignal(Operation operation, MetaTypes.OperationStateType headerState) {
        super(operation);
        this.headerState = headerState;
    }

    public MetaTypes.OperationStateType getState() {
        return headerState;
    }
}
