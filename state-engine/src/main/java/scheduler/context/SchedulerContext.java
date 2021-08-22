package scheduler.context;

import scheduler.Request;
import scheduler.statemanager.PartitionStateManager;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedDeque;


public abstract class SchedulerContext<SchedulingUnit> {
    public int thisThreadId;
    public ArrayDeque<Request> requests;
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.

    protected SchedulerContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
    }

    public abstract boolean finished();

    protected abstract void reset();

    public void push(Request request) {
        requests.push(request);
    }

    public abstract SchedulingUnit createTask(String tableName, String pKey);
}
