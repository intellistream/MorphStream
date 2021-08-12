package transaction.scheduler.tpg;

import transaction.scheduler.Request;
import transaction.scheduler.SchedulerContext;
import transaction.scheduler.tpg.struct.OperationGroup;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class TPGContext extends SchedulerContext {
    public final PartitionStateManager partitionStateManager;
    ArrayDeque<Request> requests;
    protected final ConcurrentLinkedDeque<OperationGroup> IsolatedOG;
    protected final ConcurrentLinkedDeque<OperationGroup> OGwithChildren;

    public TPGContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        partitionStateManager = new PartitionStateManager();
        requests = new ArrayDeque<>();
        IsolatedOG = new ConcurrentLinkedDeque<>();
        OGwithChildren = new ConcurrentLinkedDeque<>();
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
