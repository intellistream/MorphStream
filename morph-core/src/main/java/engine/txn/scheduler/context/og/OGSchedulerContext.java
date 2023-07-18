package engine.txn.scheduler.context.og;

import engine.txn.scheduler.Request;
import engine.txn.scheduler.context.SchedulerContext;
import engine.txn.scheduler.struct.og.OperationChain;

import java.util.*;


public abstract class OGSchedulerContext implements SchedulerContext {
    public final ArrayDeque<OperationChain> busyWaitQueue;
    public int thisThreadId;
    public ArrayDeque<Request> requests;
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.
    public Set<OperationChain> operationChains = new HashSet<>(); // TODO: For test purpose. Can be further removed
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
        operationChains.clear();
    }

    public void redo() {
        scheduledOPs = 0;
        busyWaitQueue.clear();
    }

    public void push(Request request) {
        requests.push(request);
    }

    public abstract OperationChain createTask(String tableName, String pKey, long bid);
}
