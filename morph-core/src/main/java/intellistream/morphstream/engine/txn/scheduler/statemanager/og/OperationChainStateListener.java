package intellistream.morphstream.engine.txn.scheduler.statemanager.og;

import intellistream.morphstream.engine.txn.scheduler.struct.og.MetaTypes.DependencyType;
import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;

public interface OperationChainStateListener {

    void onOcRootStart(OperationChain operationChain);

    void onOcExecuted(OperationChain operationChain);

    void onOcParentExecuted(OperationChain operationChain, DependencyType dependencyType);
}
