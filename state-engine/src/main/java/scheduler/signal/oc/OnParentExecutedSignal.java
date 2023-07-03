package scheduler.signal.oc;

import scheduler.struct.og.Operation;
import scheduler.struct.og.MetaTypes.DependencyType;
import scheduler.struct.og.OperationChain;

public class OnParentExecutedSignal extends OperationChainSignal {
    private final DependencyType dependencyType;

    public OnParentExecutedSignal(OperationChain targetOperationChain, DependencyType dependencyType) {
        super(targetOperationChain);
        this.dependencyType = dependencyType;
    }

    public DependencyType getDependencyType() {
        return dependencyType;
    }
}
