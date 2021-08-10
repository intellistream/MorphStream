package transaction.scheduler.tpg.signal.op;

import transaction.scheduler.tpg.struct.Operation;

public class OnRootSignal extends OperationSignal {
    public OnRootSignal(Operation targetOperation) {
        super(targetOperation);
    }
}
