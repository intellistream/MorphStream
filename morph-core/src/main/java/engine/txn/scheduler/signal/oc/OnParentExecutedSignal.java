package engine.txn.scheduler.signal.oc;

import engine.txn.scheduler.struct.og.MetaTypes;
import engine.txn.scheduler.struct.og.OperationChain;

public class OnParentExecutedSignal extends OperationChainSignal {
    private final MetaTypes.DependencyType dependencyType;

    public OnParentExecutedSignal(OperationChain targetOperationChain, MetaTypes.DependencyType dependencyType) {
        super(targetOperationChain);
        this.dependencyType = dependencyType;
    }

    public MetaTypes.DependencyType getDependencyType() {
        return dependencyType;
    }
}
