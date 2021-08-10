package transaction.scheduler.tpg;

import transaction.scheduler.Request;
import transaction.scheduler.SchedulerContext;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationChain;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TPGContext extends SchedulerContext {
    public final PartitionStateManager partitionStateManager;
    ArrayDeque<Request> requests;
    protected final ConcurrentLinkedDeque<OperationChain> IsolatedOC;
    protected final ConcurrentLinkedDeque<OperationChain> OCwithChildren;

    public TPGContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        partitionStateManager = new PartitionStateManager();
        requests = new ArrayDeque<>();
        IsolatedOC = new ConcurrentLinkedDeque<>();
        OCwithChildren = new ConcurrentLinkedDeque<>();
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
