package scheduler.oplevel.context;


import scheduler.oplevel.statemanager.OperationStateListener;
import scheduler.oplevel.statemanager.PartitionStateManager;
import scheduler.oplevel.statemanager.PartitionStateManagerWithAbort;
import scheduler.oplevel.struct.Operation;
import scheduler.oplevel.struct.OperationChain;

import java.util.ArrayDeque;

public class OPGSTPGContextWithAbort extends OPGSTPGContext {
    public final PartitionStateManagerWithAbort partitionStateManager;
    public final ArrayDeque<Operation> taskQueues; // task queues to store operations for each thread

    public OPGSTPGContextWithAbort(int thisThreadId) {
        super(thisThreadId);
        taskQueues = new ArrayDeque<>();
        partitionStateManager = new PartitionStateManagerWithAbort();
    }

    @Override
    public void reset() {
        super.reset();
        taskQueues.clear();
    }

    @Override
    public OperationChain createTask(String tableName, String pKey) {
        return new OperationChain(tableName, pKey);
    }

    public OperationStateListener getListener() {
        return partitionStateManager;
    }
}