package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.transNFV.common.S2PLLockObject;
import intellistream.morphstream.transNFV.vnf.UDF;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A global state manager that control concurrent access from Offload Executors to states under Offload-CC
 * */

public class OffloadSVCCStateManager {
    private final Map<Integer, S2PLLockObject> lockTable = new ConcurrentHashMap<>();
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int numExecutors = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
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

    public void executeTransaction(VNFRequest request) {
        int tupleID = request.getTupleID();
        long timeStamp = request.getCreateTime();
        String type = request.getType();

        try {
            if (Objects.equals(type, "Read")) {
                storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID)).content_.readPreValues(timeStamp).getValues().get(1).getInt();
            } else if (Objects.equals(type, "Write")) {
                TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                SchemaRecord tempo_record = new SchemaRecord(readRecord);
                tempo_record.getValues().get(1).setInt(-1);
                tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
            } else if (Objects.equals(type, "Read-Write")) {
                TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                int readValue = readRecord.getValues().get(1).getInt();
                SchemaRecord tempo_record = new SchemaRecord(readRecord);
                tempo_record.getValues().get(1).setInt(readValue);
                tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
            } else {
                throw new UnsupportedOperationException();
            }

            UDF.executeUDF(request); // Simulated UDF execution

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

}
