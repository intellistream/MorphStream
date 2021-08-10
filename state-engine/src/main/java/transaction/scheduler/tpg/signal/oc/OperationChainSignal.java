package transaction.scheduler.tpg.signal.oc;

import transaction.scheduler.tpg.signal.NotificationSignal;
import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.OperationChain;

import javax.annotation.Nullable;

public abstract class OperationChainSignal implements NotificationSignal {
    private final OperationChain targetOperationChain;

    public OperationChainSignal(OperationChain targetOperationChain) {
        this.targetOperationChain = targetOperationChain;
    }

    public OperationChain getTargetOperationChain() {
        return targetOperationChain;
    }
}
