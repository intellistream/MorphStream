package intellistream.morphstream.engine.txn.scheduler.signal.op;

import intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;

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
