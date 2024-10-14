package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.common.S2PLLockObject;
import intellistream.morphstream.transNFV.common.VNFRequest;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Each instance is guarded by its corresponding local state manager. Used in partitioning and replication
 * */

public class LocalSVCCStateManager {
    private final int instanceID;
    private final Map<Integer, S2PLLockObject> lockTable = new ConcurrentHashMap<>();
    private final LocalSVCCDatastore localSVCCDatastore = new LocalSVCCDatastore("testTable");
    private final int numExecutors = MorphStreamEnv.get().configuration().getInt("numInstances");
//    private static final int numExecutors = 2;

    public LocalSVCCStateManager(int instanceID) {
        this.instanceID = instanceID;
    }

    public void acquireLock(int key, long timestamp, boolean isWrite) throws InterruptedException {
        S2PLLockObject lockObject = lockTable.computeIfAbsent(key, k -> new S2PLLockObject(numExecutors));
        lockObject.acquireLock(timestamp, isWrite);
    }

    public void releaseLock(int key, long timestamp) {
        S2PLLockObject lockObject = lockTable.get(key);
        if (lockObject != null) {
            lockObject.releaseLock(timestamp);
//            if (lockObject.isFree()) {
//                lockTable.remove(key);
//            }
        }
    }

    public void nonBlockingTxnExecution(VNFRequest request) {
        int tupleID = request.getTupleID();
        long timeStamp = request.getCreateTime();
        int value = request.getValue();
        String type = request.getType();

        if (Objects.equals(type, "Read")) {
            localSVCCDatastore.readLocalState(tupleID);
        } else if (Objects.equals(type, "Write")) {
            localSVCCDatastore.writeLocalState(tupleID, value);
            UDF.executeUDF(request); // Simulated UDF execution
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /** Warning: This is not thread-safe, need to be guarded with timeout or other sync mechanisms */
    public void nonSafeLocalStateUpdate(int key, int value) {
        localSVCCDatastore.writeLocalState(key, value);
    }

    /** Warning: This is not thread-safe, need to be guarded with timeout or other sync mechanisms */
    public int nullSafeStateRead(int key) {
        return localSVCCDatastore.readLocalState(key);
    }

    public LocalSVCCDatastore getLocalSVCCDatastore() {
        return localSVCCDatastore;
    }
}
