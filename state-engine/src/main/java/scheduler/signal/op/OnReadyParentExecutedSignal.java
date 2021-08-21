package scheduler.signal.op;

import scheduler.struct.MetaTypes;
import scheduler.struct.bfs.BFSOperation;

/**
 * this signal is used for read operation to elect a new ready candidate.
 */
public class OnReadyParentExecutedSignal extends OperationSignal {
    private final MetaTypes.DependencyType dependencyType;
    private final MetaTypes.OperationStateType parentState;

    public OnReadyParentExecutedSignal(BFSOperation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        super(operation);
        this.dependencyType = dependencyType;
        this.parentState = parentState;
    }
}
