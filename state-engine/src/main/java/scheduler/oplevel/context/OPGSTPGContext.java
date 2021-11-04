package scheduler.oplevel.context;


import scheduler.oplevel.statemanager.OperationStateListener;
import scheduler.oplevel.statemanager.PartitionStateManager;
import scheduler.oplevel.struct.Operation;
import scheduler.oplevel.struct.OperationChain;

import java.util.ArrayDeque;

public class OPGSTPGContext extends OPSchedulerContext {
    public final PartitionStateManager partitionStateManager;
    public final ArrayDeque<Operation> IsolatedOC; // task queues to store operations for each thread
    public final ArrayDeque<Operation> OCwithChildren; // task queues to store operations for each thread


    public OPGSTPGContext(int thisThreadId) {
        super(thisThreadId);
        IsolatedOC = new ArrayDeque<>();
        OCwithChildren = new ArrayDeque<>();
        partitionStateManager = new PartitionStateManager();
    }

    @Override
    public void reset() {
        super.reset();
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