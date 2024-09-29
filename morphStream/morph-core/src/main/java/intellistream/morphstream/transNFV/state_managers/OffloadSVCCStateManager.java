package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.transNFV.common.S2PLLockObject;
import intellistream.morphstream.transNFV.vnf.UDF;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A global state manager that control concurrent access from Offload Executors to states under Offload-CC
 * */

public class OffloadSVCCStateManager {
    private final Map<Integer, S2PLLockObject> lockTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Integer> singleVersionStorage = new ConcurrentHashMap<>();
    private final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final int numExecutors = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
//    private static final int numExecutors = 2;

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

    public void svccNonBlockingTxnExecution(VNFRequest request) {
        int tupleID = request.getTupleID();
        String type = request.getType();

        if (Objects.equals(type,"Read")) {
            singleVersionStorage.get(tupleID);
        } else if (Objects.equals(type,"Write")) {
            singleVersionStorage.put(tupleID, request.getValue());
            UDF.executeUDF(request); // Simulated UDF execution
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
