package scheduler.statemanager.og;

import scheduler.struct.og.AbstractOperation;
import scheduler.struct.og.MetaTypes.DependencyType;
import scheduler.struct.og.OperationChain;

public interface OperationChainStateListener<ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>> {

    void onOcRootStart(SchedulingUnit operationChain);

    void onOcExecuted(SchedulingUnit operationChain);

    void onOcParentExecuted(SchedulingUnit operationChain, DependencyType dependencyType);
}
