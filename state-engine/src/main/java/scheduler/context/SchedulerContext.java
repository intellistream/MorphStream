package scheduler.context;

import scheduler.Request;
import scheduler.statemanager.PartitionStateManager;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedDeque;


public abstract class SchedulerContext<SchedulingUnit> {
    public final PartitionStateManager partitionStateManager;
    protected final ConcurrentLinkedDeque<SchedulingUnit> IsolatedOC;
    protected final ConcurrentLinkedDeque<SchedulingUnit> OCwithChildren;
    public int thisThreadId;
    public ArrayDeque<Request> requests;

    protected SchedulerContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        partitionStateManager = new PartitionStateManager();
        IsolatedOC = new ConcurrentLinkedDeque<>();
        OCwithChildren = new ConcurrentLinkedDeque<>();
    }

    public abstract boolean finished();

    protected abstract void reset();

    public void UpdateMapping(String key) {
        partitionStateManager.partition.add(key);
    }

    public void push(Request request) {
        requests.push(request);
    }

    public abstract SchedulingUnit createTask(String tableName, String pKey);
}
