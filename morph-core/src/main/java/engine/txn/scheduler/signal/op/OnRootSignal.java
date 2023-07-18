package engine.txn.scheduler.signal.op;


import engine.txn.scheduler.struct.op.Operation;

public class OnRootSignal extends OperationSignal {
    public OnRootSignal(Operation targetOperation) {
        super(targetOperation);
    }
}
