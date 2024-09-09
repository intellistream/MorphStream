package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.transNFV.common.VersionControl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A global state manager that control concurrent access from Offload Executors to states under Offload-CC
 * */

public class OffloadMVCCStateManager {
    // Map to store versions of each key's value and their locks
    private final Map<Integer, VersionControl> versionedStateTable = new ConcurrentHashMap<>();

    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int numExecutors = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
    private static final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");

    public OffloadMVCCStateManager() {
        for (int tupleID = 0; tupleID < NUM_ITEMS; tupleID++) {
            try {
                TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                versionedStateTable.put(tupleID, new VersionControl(tableRecord));
            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void write(int key, int value, long timestamp) throws InterruptedException {
        VersionControl versionedValue = versionedStateTable.get(key);
        if (versionedValue == null) {
            throw new RuntimeException("Key does not exist");
        }

        versionedValue.lock();
        try {
            versionedValue.enqueueWriteRequest(key, value, timestamp, true);

            // Only allow the write with the smallest timestamp
            while (!versionedValue.canGrantWrite(timestamp)) {
                versionedValue.awaitWrite();
            }

            versionedValue.commitWrite(timestamp, value);
            versionedValue.signalAll();
        } finally {
            versionedValue.unlock();
        }
    }

    public int read(int key, long timestamp) throws InterruptedException {
        VersionControl versionedValue = versionedStateTable.get(key);
        if (versionedValue == null) {
            throw new RuntimeException("Key does not exist");
        }

        versionedValue.lock();
        try {
            // Only allow reads if the timestamp is smaller than the LWM
            while (versionedValue.isWriting() && !versionedValue.canGrantRead(timestamp)) {
                versionedValue.awaitWrite();
            }

            return versionedValue.getVersion(timestamp);
        } finally {
            versionedValue.unlock();
        }
    }
}
