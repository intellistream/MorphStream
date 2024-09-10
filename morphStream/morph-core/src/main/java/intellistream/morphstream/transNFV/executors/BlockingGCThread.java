package intellistream.morphstream.transNFV.executors;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.StorageManager;

import java.util.concurrent.Callable;

public class BlockingGCThread implements Callable<Void> {

    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");

    private long lastBatchEndTimestamp;

    public BlockingGCThread(long lastBatchEndTimestamp) {
        this.lastBatchEndTimestamp = lastBatchEndTimestamp;
    }

    @Override
    public Void call() throws Exception {
        // Perform garbage collection for the specified timestamp
        for (int tupleID = 0; tupleID < NUM_ITEMS; tupleID++) {
            garbageCollection(tupleID, lastBatchEndTimestamp);
        }
        return null; // Task complete, return null
    }

    private void garbageCollection(int tupleID, long lastBatchEndTimestamp) {
        try {
            storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID)).content_.garbageCollect(lastBatchEndTimestamp);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }
}
