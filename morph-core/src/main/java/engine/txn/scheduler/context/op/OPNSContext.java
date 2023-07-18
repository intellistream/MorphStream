package engine.txn.scheduler.context.op;


import engine.txn.scheduler.statemanager.op.OperationStateListener;
import engine.txn.scheduler.statemanager.op.PartitionStateManager;
import engine.txn.scheduler.struct.op.Operation;
import engine.txn.scheduler.struct.op.OperationChain;

import java.util.ArrayDeque;

public class OPNSContext extends OPSchedulerContext {
    public final PartitionStateManager partitionStateManager;
    public final ArrayDeque<Operation> taskQueues; // task queues to store operations for each thread
    public final ArrayDeque<Operation> IsolatedOC; // task queues to store operations for each thread
    public final ArrayDeque<Operation> OCwithChildren; // task queues to store operations for each thread


    public OPNSContext(int thisThreadId) {
        super(thisThreadId);
        taskQueues = new ArrayDeque<>();
        IsolatedOC = new ArrayDeque<>();
        OCwithChildren = new ArrayDeque<>();
        partitionStateManager = new PartitionStateManager();
    }

    @Override
    public void reset() {
        super.reset();
        taskQueues.clear();
        IsolatedOC.clear();
        OCwithChildren.clear();
    }

    @Override
    public void redo() {
        super.redo();
        taskQueues.clear();
        IsolatedOC.clear();
        OCwithChildren.clear();
    }

    @Override
    public OperationChain createTask(String tableName, String pKey) {
        return new OperationChain(tableName, pKey);
    }

    public OperationStateListener getListener() {
        return partitionStateManager;
    }
}