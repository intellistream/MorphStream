package engine.txn.scheduler.signal.op;

import engine.txn.scheduler.struct.op.MetaTypes;
import engine.txn.scheduler.struct.op.Operation;

public class OnParentUpdatedSignal extends OperationSignal {
    private final MetaTypes.DependencyType dependencyType;
    private final MetaTypes.OperationStateType parentState;

    public OnParentUpdatedSignal(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        super(operation);
        this.dependencyType = dependencyType;
        this.parentState = parentState;
    }

    public MetaTypes.DependencyType getType() {
        return dependencyType;
    }

    public MetaTypes.OperationStateType getState() {
        return parentState;
    }
}
