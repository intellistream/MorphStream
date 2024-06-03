package intellistream.morphstream.engine.txn.scheduler.context.ds;

import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

public class OCCContext implements SchedulerContext {
    public final int thisThreadId;
    public ArrayDeque<Request> requests;//functions in one DAG
    private final Queue<RemoteObject> remoteObjectsBuffer = new ArrayDeque<>();

    public HashMap<String, RemoteObject> tempRemoteObjectMap = new HashMap<>();
    public final transient List<Operation> tempOperations = new ArrayList<>();
    public OCCContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
    }

    public void push(Request request) {
        requests.push(request);
        Operation operation = new Operation(request.write_key, request.table_name, request.txn_context, request.txn_context.getBID(),
                request.accessType, request.condition_records.keySet(), request.function);
        tempOperations.add(operation);
        tempRemoteObjectMap.put(operation.pKey, getRemoteObject());
    }
    private RemoteObject getRemoteObject() {
        if (remoteObjectsBuffer.isEmpty())
            return new RemoteObject();
        return remoteObjectsBuffer.poll().clear();
    }
    public void clear() {
        remoteObjectsBuffer.addAll(tempRemoteObjectMap.values());
        tempRemoteObjectMap.clear();
        tempOperations.clear();
    }

    public boolean hasUnExecuted() {
        for (Operation operation : tempOperations) {
            if (operation.operationType.equals(MetaTypes.OperationStateType.BLOCKED))
                return true;
        }
        return false;
    }
    public boolean successfullyCommit() {
        for (Operation operation : tempOperations) {
            if (!operation.operationType.equals(MetaTypes.OperationStateType.COMMITTED))
                return false;
        }
        return true;
    }
    public boolean canCommit() {
        for (Operation operation : tempOperations) {
            if (operation.operationType.equals(MetaTypes.OperationStateType.ABORTED))
                return false;
        }
        return true;
    }
    @Setter @Getter
    public static class RemoteObject {
        boolean successLocked;
        String value;
        int version;
        short validationVersion;
        public RemoteObject() {
            this.successLocked = false;
            this.value = null;
        }
        public RemoteObject clear() {
            this.successLocked = false;
            this.value = null;
            return this;
        }
    }
}
