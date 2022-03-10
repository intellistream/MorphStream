package scheduler.signal.op;

import scheduler.struct.op.MetaTypes;
import scheduler.struct.op.Operation;

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
