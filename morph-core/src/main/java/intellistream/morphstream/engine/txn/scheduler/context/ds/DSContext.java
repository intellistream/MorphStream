package intellistream.morphstream.engine.txn.scheduler.context.ds;

import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.OperationChain;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;

public class DSContext implements SchedulerContext {
    public int thisThreadId;
    public ArrayDeque<Request> requests;//functions in one DAG
    public final transient HashMap<String, Operation> tempOperationMap = new HashMap<>();//temp map for operations to set up dependencies
    private final Deque<OperationChain> allocatedTasks = new ArrayDeque<>();
    private int totalOperations = 0;
    public int scheduledOperations = 0;
    public OperationChain ready_oc;
    public DSContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        this.requests = new ArrayDeque<>();
    }
    public void push(Request request) {
        requests.push(request);
    }
    public void next() {
        if (ready_oc != null) {
            allocatedTasks.remove(ready_oc);
            if (!ready_oc.isFinished()) {
                allocatedTasks.addLast(ready_oc);
            }
        }
        ready_oc = allocatedTasks.peekFirst();
    }

    public OperationChain createTask(String tableName, String key) {
        return new OperationChain(tableName, key);
    }

    public void addTasks(OperationChain oc) {
        this.allocatedTasks.add(oc);
        totalOperations = totalOperations + oc.operations.size();
    }
    public boolean isFinished() {
        assert scheduledOperations <= totalOperations;
        return scheduledOperations == totalOperations;
    }

    public void reset() {
        scheduledOperations = 0;
        totalOperations = 0;
        ready_oc = null;
    }
}
