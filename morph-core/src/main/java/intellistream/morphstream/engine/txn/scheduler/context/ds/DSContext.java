package intellistream.morphstream.engine.txn.scheduler.context.ds;

import intellistream.morphstream.api.input.statistic.DriverSideOwnershipTable;
import intellistream.morphstream.api.input.statistic.WorkerSideOwnershipTable;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.OperationChain;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class DSContext implements SchedulerContext {
    private final static Logger LOG = Logger.getLogger(DSContext.class);
    public int thisThreadId;
    private final int totalWorker;
    private final List<Integer> receivedWorker = new ArrayList<>();
    private Tuple2<Long, ByteBuffer> tempCanRead;//the temp buffer to decide whether the remote operations can read
    private ByteBuffer remoteOperationBuffer;
    private final List<Operation> remoteOperations = new ArrayList<>();
    public ArrayDeque<Request> requests;//functions in one DAG
    public final transient HashMap<String, Operation> tempOperationMap = new HashMap<>();//temp map for operations to set up dependencies
    private final Deque<OperationChain> allocatedTasks = new ArrayDeque<>();
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
        remoteOperations.clear();
        receivedWorker.clear();
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
                            remoteOperations.add(new Operation(operationInfo[1], operationInfo[2], Long.parseLong(operationInfo[0]), true));
                            operationNum ++;
                        }
                        receivedWorker.add(i);
                        LOG.info(String.format("Thread(%d) receive remote %d operations from worker(%d) ",this.thisThreadId, operationNum, i));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return remoteOperations;
    }
}
