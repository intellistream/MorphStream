package scheduler.oplevel.signal.op;


import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;

public class OnRollbackAndRedoSignal extends OperationSignal {
    private final MetaTypes.DependencyType dependencyType;
    private final MetaTypes.OperationStateType parentState;

    public OnRollbackAndRedoSignal(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
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
