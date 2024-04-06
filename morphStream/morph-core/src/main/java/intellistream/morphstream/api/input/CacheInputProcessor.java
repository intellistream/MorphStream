package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.util.libVNFFrontend.NativeInterface;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CacheInputProcessor implements Runnable {
    private final BlockingQueue<byte[]> operationQueue;

    public CacheInputProcessor(BlockingQueue<byte[]> operationQueue) {
        this.operationQueue = operationQueue;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] txnByteArray = operationQueue.poll();
            long txnID = 0;
            String tupleID = "0"; //TODO: Hardcoded
            int value = 0;
            NativeInterface.__update_states_to_cache(tupleID, value); //Sync state updates to all caches
        }
    }
}
