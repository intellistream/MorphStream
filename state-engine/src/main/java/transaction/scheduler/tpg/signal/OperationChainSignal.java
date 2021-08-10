package transaction.scheduler.tpg.signal;

import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.OperationChain;

import javax.annotation.Nullable;

public class OperationChainSignal implements NotificationSignal {
    private final OperationChain targetOperationChain;
    private final DependencyType dependencyType;

    public OperationChainSignal(OperationChain targetOperationChain, @Nullable DependencyType dependencyType) {
        this.targetOperationChain = targetOperationChain;
        this.dependencyType = dependencyType;
    }

    public OperationChain getTargetOperationChain() {
        return targetOperationChain;
    }
    public DependencyType getDependencyType() {
        return dependencyType;
    }
}
