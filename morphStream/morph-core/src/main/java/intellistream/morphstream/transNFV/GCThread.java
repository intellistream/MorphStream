package intellistream.morphstream.transNFV;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.StorageManager;

import java.util.HashMap;

public class GCThread implements Runnable {

    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int gcInterval = MorphStreamEnv.get().configuration().getInt("gcInterval");
    private static final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private static final HashMap<String, Integer> executorEndTimestamps = new HashMap<>();
    private int currentGCInterval = 0;

    private void garbageCollection(int tupleID, int timestamp) {
        try {
            storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID)).content_.garbageCollect(timestamp);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            // Periodically fetch the latest operation timestamps from executors
            // TODO: Garbage identification: When all executors finish processing the current GC_batch
            // Garbage collection
            currentGCInterval += gcInterval;
            for (int i = 0; i < NUM_ITEMS; i++) {
                garbageCollection(i, currentGCInterval);
            }
        }
    }
}
