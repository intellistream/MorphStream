package transaction.scheduler.tpg.signal.og;

import transaction.scheduler.tpg.struct.OperationGroup;

public class OnExecutedSignal extends OperationGroupSignal {
    public OnExecutedSignal(OperationGroup targetOperationGroup) {
        super(targetOperationGroup);
    }
}
