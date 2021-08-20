package scheduler.signal.oc;

import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.bfs.BFSOperationChain;

import javax.annotation.Nullable;

public class OnParentExecutedSignal extends OperationChainSignal {
    private final DependencyType dependencyType;

    public OnParentExecutedSignal(BFSOperationChain targetOperationChain, @Nullable DependencyType dependencyType) {
        super(targetOperationChain);
        this.dependencyType = dependencyType;
    }

    public DependencyType getDependencyType() {
        return dependencyType;
    }
}
