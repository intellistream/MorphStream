package scheduler.statemanager;

import scheduler.struct.AbstractOperation;
import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.OperationChain;

public interface OperationChainStateListener<ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>> {

    void onOcRootStart(SchedulingUnit operationChain);

    void onOcExecuted(SchedulingUnit operationChain);

    void onOcParentExecuted(SchedulingUnit operationChain, DependencyType dependencyType);
}
