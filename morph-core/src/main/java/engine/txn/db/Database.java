package engine.txn.db;

import engine.txn.durability.ftmanager.FTManager;
import engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import engine.txn.durability.recovery.RedoLogResult;
import engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import engine.txn.storage.EventManager;
import engine.txn.storage.StorageManager;
import engine.txn.storage.TableRecord;
import engine.txn.storage.table.RecordSchema;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class Database {
    public int numTransactions = 0;//current number of activate transactions
    StorageManager storageManager;
    EventManager eventManager;
    LoggingManager loggingManager;
    public EventManager getEventManager() {
        return eventManager;
    }
    public LoggingManager getLoggingManager() { return loggingManager;}
//	public transient TxnParam param;

    /**
     * Close this database.
     */
    public synchronized void close() throws IOException {
        storageManager.close();
    }

    /**
     *
     */
    public void dropAllTables() throws IOException {
        storageManager.dropAllTables();
    }

    /**
     * @param tableSchema
     * @param tableName
     */
    public void createTable(RecordSchema tableSchema, String tableName, int partition_num, int num_items) {
        try {
            storageManager.createTable(tableSchema, tableName, partition_num, num_items);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    public abstract void InsertRecord(String table, TableRecord record, int partition_id) throws DatabaseException;

    public StorageManager getStorageManager() {
        return storageManager;
    }

    /**
     * Used to implement fault tolerance
     */
    public abstract void asyncSnapshot(final long snapshotId, final int partitionId, final FTManager ftManager) throws IOException;
    public abstract void asyncCommit(final long groupId, final int partitionId, final FTManager ftManager) throws IOException;
    public abstract void syncReloadDB(SnapshotResult snapshotResult) throws IOException, ExecutionException, InterruptedException;
    public abstract void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException;
}
