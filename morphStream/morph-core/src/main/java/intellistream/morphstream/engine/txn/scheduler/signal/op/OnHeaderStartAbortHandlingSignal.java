package intellistream.morphstream.engine.txn.scheduler.signal.op;

import intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;

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
