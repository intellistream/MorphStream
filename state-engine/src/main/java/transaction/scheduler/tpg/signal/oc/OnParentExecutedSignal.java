package transaction.scheduler.tpg.signal.oc;

import transaction.scheduler.tpg.signal.NotificationSignal;
import transaction.scheduler.tpg.signal.op.OperationSignal;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.OperationChain;

import javax.annotation.Nullable;

public class OnParentExecutedSignal extends OperationChainSignal {
    private final DependencyType dependencyType;

    public OnParentExecutedSignal(OperationChain targetOperationChain, @Nullable DependencyType dependencyType) {
        super(targetOperationChain);
        this.dependencyType = dependencyType;
    }

    public DependencyType getDependencyType() {
        return dependencyType;
    }
}
