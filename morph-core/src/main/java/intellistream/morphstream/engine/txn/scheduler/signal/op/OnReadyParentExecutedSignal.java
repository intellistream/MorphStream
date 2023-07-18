package intellistream.morphstream.engine.txn.scheduler.signal.op;

import intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;

/**
 * this signal is used for read operation to elect a new ready candidate.
 */
public class OnReadyParentExecutedSignal extends OperationSignal {
    private final MetaTypes.DependencyType dependencyType;
    private final MetaTypes.OperationStateType parentState;

    public OnReadyParentExecutedSignal(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        super(operation);
        this.dependencyType = dependencyType;
        this.parentState = parentState;
    }
}
