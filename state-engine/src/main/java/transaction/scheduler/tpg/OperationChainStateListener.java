package transaction.scheduler.tpg;

import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.OperationChain;

public interface OperationChainStateListener {

    void onOcRootStart(OperationChain operationChain);

    void onOcExecuted(OperationChain operationChain);

    void onOcParentExecuted(OperationChain operationChain, MetaTypes.DependencyType dependencyType);
}
