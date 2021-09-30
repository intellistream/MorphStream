package scheduler.oplevel.context;



import scheduler.Request;
import scheduler.oplevel.statemanager.PartitionStateManager;
import scheduler.oplevel.struct.Operation;
import scheduler.oplevel.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class OPGSTPGContext extends OPSchedulerContext {
    public final PartitionStateManager partitionStateManager;
    public final ConcurrentLinkedDeque<Operation> taskQueues; // task queues to store operations for each thread
    public final ArrayDeque<Operation> batchedOperations;
    public ArrayDeque<Request> requests;

    public OPGSTPGContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        taskQueues = new ConcurrentLinkedDeque<>();
        batchedOperations = new ArrayDeque<>();
        partitionStateManager = new PartitionStateManager();
        requests = new ArrayDeque<>();
    }

    @Override
    public void UpdateMapping(String key) {
        partitionStateManager.partition.add(key);
    }

    @Override
    protected boolean finished() {
        return false;
    }

    @Override
    protected void reset() {

    }

    public void initialize(TaskPrecedenceGraph.ShortCutListener shortCutListener) {
        partitionStateManager.initialize(shortCutListener);
    }
}