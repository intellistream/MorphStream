package intellistream.morphstream.transNFV.executors;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.StorageManager;

import java.util.LinkedList;
import java.util.Queue;

public class NonBlockingGCThread implements Runnable {

    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private static final long STOP_SIGNAL = -1L; // Special signal to stop the GC thread

    private final Queue<Long> taskQueue = new LinkedList<>(); // Queue to hold GC tasks
    private final Object lock = new Object(); // Lock for managing thread coordination

    @Override
    public void run() {
        while (true) {
            long lastBatchEndTimestamp;
            synchronized (lock) {
                try {
                    // Wait until a GC task is available in the queue
                    while (taskQueue.isEmpty()) {
                        lock.wait(); // Wait for the signal to start GC
                    }

                    // Get the next task (lastBatchEndTimestamp) from the queue
                    lastBatchEndTimestamp = taskQueue.poll();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return; // Exit the thread if interrupted
                }
            }

            // Check if we received the stop signal
            if (lastBatchEndTimestamp == STOP_SIGNAL) {
                System.out.println("Stop signal received. Terminating GC thread.");
                return; // Exit the thread gracefully
            }

            // Perform garbage collection for the specified timestamp
            for (int tupleID = 0; tupleID < NUM_ITEMS; tupleID++) {
                garbageCollection(tupleID, lastBatchEndTimestamp);
            }

            System.out.println("GC completed for batch with timestamp: " + lastBatchEndTimestamp);
        }
    }

    public void startGC(long lastBatchEndTimestamp) {
        synchronized (lock) {
            taskQueue.offer(lastBatchEndTimestamp); // Add the GC task to the queue
            lock.notify(); // Notify the GC thread to wake up and process the task
        }
    }

    // Method to stop the GC thread by sending the stop signal
    public void stopGC() {
        startGC(STOP_SIGNAL); // Send the special stop signal to the GC thread
    }

    private void garbageCollection(int tupleID, long lastBatchEndTimestamp) {
        // Uncomment and modify for actual database garbage collection logic
        /*
        try {
            storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID)).content_.garbageCollect(lastBatchEndTimestamp);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
        */
    }
}
