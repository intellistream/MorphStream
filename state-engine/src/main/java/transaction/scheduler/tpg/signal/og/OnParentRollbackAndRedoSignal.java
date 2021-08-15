package transaction.scheduler.tpg.signal.og;

import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.OperationGroup;

import javax.annotation.Nullable;

public class OnParentRollbackAndRedoSignal extends OperationGroupSignal {
    private final DependencyType dependencyType;

    public OnParentRollbackAndRedoSignal(OperationGroup targetOperationGroup, @Nullable DependencyType dependencyType) {
        super(targetOperationGroup);
        this.dependencyType = dependencyType;
    }

    public DependencyType getDependencyType() {
        return dependencyType;
    }
}
