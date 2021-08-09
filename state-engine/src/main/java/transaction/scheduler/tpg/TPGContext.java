package transaction.scheduler.tpg;

import transaction.scheduler.Request;
import transaction.scheduler.SchedulerContext;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class TPGContext extends SchedulerContext {
    public final PartitionStateManager partitionStateManager;
    protected final ConcurrentLinkedDeque<Operation> taskQueues; // task queues to store operations for each thread
    protected final ArrayDeque<Operation> batchedOperations;
    ArrayDeque<Request> requests;

    public TPGContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        taskQueues = new ConcurrentLinkedDeque<>();
        batchedOperations = new ArrayDeque<>();
        partitionStateManager = new PartitionStateManager();

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
