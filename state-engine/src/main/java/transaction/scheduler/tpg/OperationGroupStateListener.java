package transaction.scheduler.tpg;

import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationGroup;

public interface OperationGroupStateListener {

    void onOgRootStart(OperationGroup operationGroup);

    void onOgExecuted(OperationGroup operationGroup);

    void onOgParentExecuted(OperationGroup operationGroup, MetaTypes.DependencyType dependencyType);

    void onOgRollbackAndRedo(OperationGroup operationGroup, Operation operation);

    void onOgParentRollbackAndRedo(OperationGroup operationGroup, MetaTypes.DependencyType dependencyType);
}
