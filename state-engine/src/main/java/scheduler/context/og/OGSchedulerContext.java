package scheduler.context.og;

import scheduler.Request;
import scheduler.context.SchedulerContext;

import java.util.*;


public abstract class OGSchedulerContext<SchedulingUnit> implements SchedulerContext {
    public final ArrayDeque<SchedulingUnit> busyWaitQueue;
    public int thisThreadId;
    public ArrayDeque<Request> requests;
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.
    public Set<SchedulingUnit> operationChains = new HashSet<>();
    public int fd = 0;


    protected OGSchedulerContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        requests = new ArrayDeque<>();
        busyWaitQueue = new ArrayDeque<>(); // this is used to store those ocs that does not finished
    }

    public abstract boolean finished();

    public void reset() {
        requests.clear();
        scheduledOPs = 0;
        totalOsToSchedule = 0;
        busyWaitQueue.clear();
    }

    public void redo() {
        scheduledOPs = 0;
        busyWaitQueue.clear();
    }

    public void push(Request request) {
        requests.push(request);
    }

    public abstract SchedulingUnit createTask(String tableName, String pKey, long bid);
}
