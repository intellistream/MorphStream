package scheduler.signal.oc;

import scheduler.struct.og.AbstractOperation;
import scheduler.struct.og.MetaTypes.DependencyType;
import scheduler.struct.og.OperationChain;

public class OnParentExecutedSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        extends OperationChainSignal<OP, OC> {
    private final DependencyType dependencyType;

    public OnParentExecutedSignal(OC targetOperationChain, DependencyType dependencyType) {
        super(targetOperationChain);
        this.dependencyType = dependencyType;
    }

    public DependencyType getDependencyType() {
        return dependencyType;
    }
}
