package scheduler.signal.oc;

import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.OperationChain;

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
