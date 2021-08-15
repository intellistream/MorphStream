package transaction.scheduler.tpg.signal.og;

import transaction.scheduler.tpg.struct.OperationGroup;

public class OnRollbackAndRedoSignal extends OperationGroupSignal {
    public OnRollbackAndRedoSignal(OperationGroup targetOperationGroup) {
        super(targetOperationGroup);
    }
}

