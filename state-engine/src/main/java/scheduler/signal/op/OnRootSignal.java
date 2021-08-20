package scheduler.signal.op;

import scheduler.struct.Operation;

public class OnRootSignal extends OperationSignal {
    public OnRootSignal(Operation targetOperation) {
        super(targetOperation);
    }
}
