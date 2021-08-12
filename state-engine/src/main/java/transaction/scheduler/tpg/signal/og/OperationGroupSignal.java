package transaction.scheduler.tpg.signal.og;

import transaction.scheduler.tpg.signal.NotificationSignal;
import transaction.scheduler.tpg.struct.OperationGroup;

public abstract class OperationGroupSignal implements NotificationSignal {
    private final OperationGroup targetOperationGroup;

    public OperationGroupSignal(OperationGroup targetOperationGroup) {
        this.targetOperationGroup = targetOperationGroup;
    }

    public OperationGroup getTargetOperationGroup() {
        return targetOperationGroup;
    }
}
