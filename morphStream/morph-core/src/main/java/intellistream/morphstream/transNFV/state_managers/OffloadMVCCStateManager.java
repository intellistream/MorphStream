package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.transNFV.common.OffloadVersionControl;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A global state manager that control concurrent access from Offload Executors to states under Offload-CC
 * */

public class OffloadMVCCStateManager {
    // Map to store versions of each key's value and their locks
    private final ConcurrentHashMap<Integer, OffloadVersionControl> versionedStateTable = new ConcurrentHashMap<>();

    private final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final int numExecutors = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
    private final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");

    public OffloadMVCCStateManager() {
        for (int tupleID = 0; tupleID < NUM_ITEMS; tupleID++) {
            try {
                TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                versionedStateTable.put(tupleID, new OffloadVersionControl(tableRecord));
            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            } catch (NullPointerException e) {
                throw new RuntimeException("Key does not exist: " + tupleID);
            }
        }
    }

    public void mvccWrite(VNFRequest request) throws InterruptedException {
        int key = request.getTupleID();
        int value = request.getValue();
        long timestamp = request.getCreateTime();
        OffloadVersionControl targetVersionControl = versionedStateTable.get(key);
        if (targetVersionControl == null) {
            throw new RuntimeException("Key does not exist");
        }

        targetVersionControl.lock();
        try {
            targetVersionControl.enqueueWriteRequest(key, value, timestamp, true);

            // Only allow the write with the smallest timestamp
            while (!targetVersionControl.canGrantWrite(timestamp)) {
                targetVersionControl.awaitForWrite();
            }

            // State access
            targetVersionControl.writeVersion(request);

            // Signal all waiting threads TODO: This can be improved by only signal one thread
            targetVersionControl.signalNext();

        } finally {
            targetVersionControl.unlock();
        }
    }

    public int mvccRead(VNFRequest request) throws InterruptedException {
        int key = request.getTupleID();
        long timestamp = request.getCreateTime();
        OffloadVersionControl targetVersionControl = versionedStateTable.get(key);
        if (targetVersionControl == null) {
            throw new RuntimeException("Key does not exist");
        }

        targetVersionControl.lock();
        try {
            // Only allow reads if the timestamp is smaller than the LWM
            while (!targetVersionControl.canGrantRead(timestamp)) {
                targetVersionControl.awaitForWrite();
            }

            // State access
            return targetVersionControl.readVersion(timestamp);

        }
        finally {
            targetVersionControl.unlock();
        }
    }
}
