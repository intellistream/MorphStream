package engine.txn.scheduler.context.op;


import engine.txn.scheduler.statemanager.op.OperationStateListener;
import engine.txn.scheduler.statemanager.op.PartitionStateManagerWithAbort;
import engine.txn.scheduler.struct.op.Operation;
import engine.txn.scheduler.struct.op.OperationChain;

import java.util.ArrayDeque;

public class OPNSAContext extends OPNSContext {
    public final PartitionStateManagerWithAbort partitionStateManager;
    public final ArrayDeque<Operation> taskQueues; // task queues to store operations for each thread

    public OPNSAContext(int thisThreadId) {
        super(thisThreadId);
        taskQueues = new ArrayDeque<>();
        partitionStateManager = new PartitionStateManagerWithAbort();
    }

    @Override
    public OperationChain createTask(String tableName, String pKey) {
        return new OperationChain(tableName, pKey);
    }

    public OperationStateListener getListener() {
        return partitionStateManager;
    }
}