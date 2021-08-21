package scheduler.statemanager;

import scheduler.struct.MetaTypes;
import scheduler.struct.bfs.BFSOperationChain;

public interface OperationChainStateListener {

    void onOcRootStart(BFSOperationChain operationChain);

    void onOcExecuted(BFSOperationChain operationChain);

    void onOcParentExecuted(BFSOperationChain operationChain, MetaTypes.DependencyType dependencyType);
}
