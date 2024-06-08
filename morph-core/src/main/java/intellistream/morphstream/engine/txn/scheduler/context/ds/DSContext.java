package intellistream.morphstream.engine.txn.scheduler.context.ds;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.OperationChain;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class DSContext implements SchedulerContext {
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(DSContext.class);
    public int thisThreadId;
    private final int totalWorker;
    private final List<Integer> receivedWorker = new ArrayList<>();
    private Tuple2<Long, ByteBuffer> tempCanRead;//the temp buffer to decide whether the remote operations can read
    private ByteBuffer remoteOperationBuffer;
    private final List<Operation> remoteOperations = new ArrayList<>();
    public ArrayDeque<Request> requests;//functions in one DAG
    public final transient HashMap<String, Operation> tempOperationMap = new HashMap<>();//temp map for operations to set up dependencies
    public final Deque<OperationChain> allocatedLocalTasks = new ArrayDeque<>();
    public final Deque<OperationChain> allocatedRemoteTasks = new ArrayDeque<>();
    private boolean useLocal = true;
    private int localCount = 0;
    private int remoteCount = 0;
    private int totalOperations = 0;
    public int scheduledOperations = 0;
    public OperationChain ready_oc;
    public DSContext(int thisThreadId) {
        this.thisThreadId = thisThreadId;
        this.requests = new ArrayDeque<>();
        this.totalWorker = MorphStreamEnv.get().configuration().getInt("workerNum");
    }
    public void push(Request request) {
        requests.push(request);
    }
    public void next() {
        if (useLocal) {
            if (ready_oc != null) {
                if (!ready_oc.isFinished()) {
                    allocatedLocalTasks.addLast(ready_oc);
                }
            }
        } else {
            if (ready_oc != null) {
                if (!ready_oc.isFinished()) {
                    allocatedRemoteTasks.addLast(ready_oc);
                }
            }
        }
        switchLocal();
        if (useLocal) {
            ready_oc = allocatedLocalTasks.pollFirst();
            localCount ++;
        } else {
            ready_oc = allocatedRemoteTasks.pollFirst();
            remoteCount ++;
        }
    }

    private void switchLocal() {
        if (useLocal) {
            if (localCount >= this.allocatedLocalTasks.size()) {
                if (!allocatedRemoteTasks.isEmpty())
                    useLocal = false;
                localCount = 0;
            }
        } else {
            if (remoteCount >= this.allocatedRemoteTasks.size()) {
                if (!allocatedLocalTasks.isEmpty())
                    useLocal = true;
                remoteCount = 0;
            }
        }
    }

    public OperationChain createTask(String tableName, String key) {
        return new OperationChain(tableName, key);
    }

    public void addTasks(OperationChain oc) {
        if (oc.isLocalState()) {
            this.allocatedLocalTasks.add(oc);
        } else {
            this.allocatedRemoteTasks.add(oc);
        }
        totalOperations = totalOperations + oc.operations.size();
    }
    public void setupDependencies() {
        for (OperationChain oc : this.allocatedLocalTasks) {
            if (!oc.operations.isEmpty()) {
                oc.updateDependencies();
            }
        }
        for (OperationChain oc : this.allocatedRemoteTasks) {
            if (!oc.operations.isEmpty()) {
                oc.updateDependencies();
            }
        }
    }
    public boolean isFinished() {
        assert scheduledOperations <= totalOperations;
        return scheduledOperations == totalOperations;
    }

    public void reset() {
        scheduledOperations = 0;
        totalOperations = 0;
        ready_oc = null;
        remoteOperations.clear();
        receivedWorker.clear();
        tempOperationMap.clear();
        localCount = 0;
        remoteCount = 0;
        useLocal = true;
    }
    public List<Operation> receiveRemoteOperations(RdmaWorkerManager rdmaWorkerManager) {
        while (receivedWorker.size() != totalWorker) {
            for (int i = 0; i < totalWorker; i++) {
                try {
                    if (receivedWorker.contains(i)) {
                        continue;
                    }
                    if (i == rdmaWorkerManager.getManagerId()) {
                        receivedWorker.add(i);
                        continue;
                    }
                    tempCanRead = rdmaWorkerManager.getRemoteOperationsBuffer(i).canRead(this.thisThreadId);
                    if (tempCanRead != null) {
                        List<Integer> lengthQueue = new ArrayList<>();
                        while(tempCanRead._2().hasRemaining()) {
                            lengthQueue.add(tempCanRead._2().getInt());
                        }
                        long myOffset = tempCanRead._1();
                        int myLength = lengthQueue.get(this.thisThreadId);
                        for (int j = 0; j < this.thisThreadId; j++) {
                            myOffset += lengthQueue.get(j);
                        }
                        remoteOperationBuffer = rdmaWorkerManager.getRemoteOperationsBuffer(i).read(myOffset, myLength);
                        int operationNum = 0;
                        while (remoteOperationBuffer.hasRemaining()) {
                            int operationSize = remoteOperationBuffer.getInt();
                            byte[] bytes = new byte[operationSize];
                            remoteOperationBuffer.get(bytes);
                            String[] operationInfo = new String(bytes).split(":");//bid, table, pk
                            remoteOperations.add(new Operation(operationInfo[1], operationInfo[2], Long.parseLong(operationInfo[0]), true, i));
                            operationNum ++;
                        }
                        receivedWorker.add(i);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return remoteOperations;
    }
}
