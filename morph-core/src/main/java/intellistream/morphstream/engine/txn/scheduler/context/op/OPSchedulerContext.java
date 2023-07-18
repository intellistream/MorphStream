package intellistream.morphstream.engine.txn.scheduler.context.op;

import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.statemanager.op.OperationStateListener;
import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.op.OperationChain;

import java.util.ArrayDeque;

public abstract class OPSchedulerContext implements SchedulerContext {
    public final ArrayDeque<Operation> batchedOperations;
    public int thisThreadId;
    public ArrayDeque<Request> requests;
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.
    public ArrayDeque<Operation> operations = new ArrayDeque<>();
    public int fd = 0;

    protected OPSchedulerContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        requests = new ArrayDeque<>();
        batchedOperations = new ArrayDeque<>();
    }

    public boolean finished() {
        assert scheduledOPs <= totalOsToSchedule;
        return scheduledOPs == totalOsToSchedule;
    }

    public void reset() {
        requests.clear();
        scheduledOPs = 0;
        totalOsToSchedule = 0;
        operations.clear();
        batchedOperations.clear();
    }

    public void redo() {
        requests.clear();
        scheduledOPs = 0;
        operations.clear();
        batchedOperations.clear();
    }

    public void push(Request request) {
        requests.push(request);
    }

    public abstract OperationChain createTask(String tableName, String pKey);

    public OperationStateListener getListener() {
        throw new UnsupportedOperationException();
    }
}
