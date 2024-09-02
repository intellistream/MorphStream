package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.transNFV.data.S2PLLockObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A global state manager that control concurrent access from Offload Executors to states under Offload-CC
 * */

public class OffloadSVCCStateManager {
    private final Map<String, S2PLLockObject> lockTable = new ConcurrentHashMap<>();
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int numExecutors = MorphStreamEnv.get().configuration().getInt("offloadCCThreadNum");
//    private static final int numExecutors = 2;

    public void acquireLock(String key, long timestamp, boolean isWrite) throws InterruptedException {
        S2PLLockObject lockObject = lockTable.computeIfAbsent(key, k -> new S2PLLockObject(numExecutors));
        lockObject.acquireLock(timestamp, isWrite);
    }

    public void releaseLock(String key, long timestamp) {
        S2PLLockObject lockObject = lockTable.get(key);
        if (lockObject != null) {
            lockObject.releaseLock(timestamp);
            if (lockObject.isFree()) {
                lockTable.remove(key);
            }
        }
    }

    public void executeTransaction(VNFRequest request) {
        int tupleID = request.getTupleID();
        long timeStamp = request.getCreateTime();

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            VNFManagerUDF.executeUDF(request);
            readValue += 1;

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(readValue);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

}
