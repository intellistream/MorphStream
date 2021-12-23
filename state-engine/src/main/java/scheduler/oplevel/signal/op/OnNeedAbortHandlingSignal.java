package scheduler.oplevel.signal.op;

import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;

public class OnNeedAbortHandlingSignal extends OperationSignal {
    private final MetaTypes.OperationStateType headerState;

    public OnNeedAbortHandlingSignal(Operation operation, MetaTypes.OperationStateType headerState) {
        super(operation);
        this.headerState = headerState;
    }

    public MetaTypes.OperationStateType getState() {
        return headerState;
    }
}
