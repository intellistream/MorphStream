package scheduler.context;

import org.apache.hadoop.util.hash.Hash;
import scheduler.Request;
import scheduler.struct.AbstractOperation;

import java.util.*;


public abstract class SchedulerContext<SchedulingUnit> {
    public int thisThreadId;
    public ArrayDeque<Request> requests;
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.
    public Set<AbstractOperation> operaitonsLeft = new HashSet<>();//total number of operations to process per thread.
    public ArrayDeque<SchedulingUnit> cirularOCs;


    protected SchedulerContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        requests = new ArrayDeque<>();
        cirularOCs = new ArrayDeque<>();
    }

    public abstract boolean finished();

    public void reset() {
        requests.clear();
        scheduledOPs = 0;
        totalOsToSchedule = 0;
        cirularOCs.clear();
    }

    public void push(Request request) {
        requests.push(request);
    }

    public abstract SchedulingUnit createTask(String tableName, String pKey);
}
