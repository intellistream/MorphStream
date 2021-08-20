package transaction.scheduler.tpg;

import scheduler.struct.MetaTypes;
import scheduler.struct.OperationChain;

public interface OperationChainStateListener {

    void onOcRootStart(OperationChain operationChain);

    void onOcExecuted(OperationChain operationChain);

    void onOcParentExecuted(OperationChain operationChain, MetaTypes.DependencyType dependencyType);
}
