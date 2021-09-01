package scheduler.context;

import scheduler.Request;

import java.util.ArrayDeque;


public abstract class SchedulerContext<SchedulingUnit> {
    public int thisThreadId;
    public ArrayDeque<Request> requests;
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.

    protected SchedulerContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        requests = new ArrayDeque<>();
    }

    public abstract boolean finished();

    public void reset() {
        requests.clear();
        scheduledOPs = 0;
        totalOsToSchedule = 0;
    }

    public void push(Request request) {
        requests.push(request);
    }

    public abstract SchedulingUnit createTask(String tableName, String pKey);
}
