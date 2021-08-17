package transaction.scheduler.tpg.signal.og;

import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationGroup;

import javax.annotation.Nullable;

public class OnParentWithAbortedRollbackAndRedoSignal extends OperationGroupSignal {
    private final DependencyType dependencyType;
    private final Operation targetChildOperation;
    private final Operation abortedOperation;

    public OnParentWithAbortedRollbackAndRedoSignal(OperationGroup targetOperationGroup, @Nullable DependencyType dependencyType, Operation targetChildOperation, Operation abortedOperation) {
        super(targetOperationGroup);
        this.dependencyType = dependencyType;
        this.targetChildOperation = targetChildOperation;
        this.abortedOperation = abortedOperation;
    }

    public DependencyType getDependencyType() {
        return dependencyType;
    }

    public Operation getAbortedOperation() {
        return abortedOperation;
    }

    public Operation getTargetChildOperation() {
        return targetChildOperation;
    }
}
