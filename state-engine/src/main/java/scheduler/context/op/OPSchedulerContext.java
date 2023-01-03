package scheduler.context.op;

import scheduler.Request;
import scheduler.context.SchedulerContext;
import scheduler.statemanager.op.OperationStateListener;
import scheduler.struct.op.Operation;
import scheduler.struct.op.OperationChain;
import stage.Stage;
import utils.lib.ConcurrentHashMap;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class OPSchedulerContext implements SchedulerContext {
    public final ArrayDeque<Operation> batchedOperations;
    private final Stage stage;
    public int thisThreadId;
//    private final Stage stage;

//    public ArrayDeque<Request> requests; //TODO: Change back after testing
    public ConcurrentLinkedDeque<Request> requests;
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.
    public ArrayDeque<Operation> operations = new ArrayDeque<>();
    public int fd = 0;

    protected OPSchedulerContext(int thisThreadId, Stage stage) {
        this.thisThreadId = thisThreadId;
        this.stage = stage;
//        requests = new ArrayDeque<>(); //TODO: Change back after testing
        requests = new ConcurrentLinkedDeque<>();
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

    public static AtomicInteger opsContextPushCounter = new AtomicInteger(0);

    public void push(Request request) {
        requests.push(request);

        if (Objects.equals(request.operator_name, "ed_sc")) { //TODO: Remove after testing
            opsContextPushCounter.incrementAndGet();
        }
    }

    public abstract OperationChain createTask(String tableName, String pKey);

    public OperationStateListener getListener() {
        throw new UnsupportedOperationException();
    }

    public void waitForOtherThreads(int thisThreadId) {
        stage.getControl().waitForOtherThreads(thisThreadId);
    }
}
