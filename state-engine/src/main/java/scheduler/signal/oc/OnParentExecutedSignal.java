package scheduler.signal.oc;

import scheduler.struct.AbstractOperation;
import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.OperationChain;

import javax.annotation.Nullable;

public class OnParentExecutedSignal<OP extends AbstractOperation, OC extends OperationChain<OP>>
        extends OperationChainSignal<OP, OC> {
    private final DependencyType dependencyType;

    public OnParentExecutedSignal(OC targetOperationChain, @Nullable DependencyType dependencyType) {
        super(targetOperationChain);
        this.dependencyType = dependencyType;
    }

    public DependencyType getDependencyType() {
        return dependencyType;
    }
}
