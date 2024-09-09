package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.common.S2PLLockObject;
import intellistream.morphstream.transNFV.common.VNFRequest;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A global state manager that control concurrent access from Offload Executors to states under Offload-CC
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

    public void executeTransaction(VNFRequest request) {
        int tupleID = request.getTupleID();
        long timeStamp = request.getCreateTime();
        int value = request.getValue();
        String type = request.getType();

        if (Objects.equals(type, "Read")) {
            localSVCCDatastore.readSVCCLocalState(tupleID);
        } else if (Objects.equals(type, "Write")) {
            localSVCCDatastore.writeSVCCLocalState(tupleID, value);
        } else if (Objects.equals(type, "Read-Write")) {
            localSVCCDatastore.readSVCCLocalState(tupleID);
            localSVCCDatastore.writeSVCCLocalState(tupleID, value);
        } else {
            throw new UnsupportedOperationException();
        }

        UDF.executeUDF(request); // Simulated UDF execution
    }

    /** Warning: This is not thread-safe, need to be guarded with timeout or other sync mechanisms */
    public void nullSafeStateUpdate(int key, int value) {
        localSVCCDatastore.writeSVCCLocalState(key, value);
    }

    /** Warning: This is not thread-safe, need to be guarded with timeout or other sync mechanisms */
    public int nullSafeStateRead(int key) {
        return localSVCCDatastore.readSVCCLocalState(key);
    }

}
