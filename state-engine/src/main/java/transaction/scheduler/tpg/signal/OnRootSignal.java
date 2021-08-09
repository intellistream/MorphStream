package transaction.scheduler.tpg.signal;

import transaction.scheduler.tpg.struct.Operation;

public class OnRootSignal extends NotificationSignal {
    public OnRootSignal(Operation targetOperation) {
        super(targetOperation);
    }
}
