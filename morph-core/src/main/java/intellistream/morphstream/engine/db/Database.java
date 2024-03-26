package intellistream.morphstream.engine.db;

import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.storage.StorageManager;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import intellistream.morphstream.engine.db.storage.impl.LocalStorageManager;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.db.storage.table.RecordSchema;
import lombok.Getter;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Getter
public abstract class Database {
    public StorageManager storageManager;
    public LoggingManager loggingManager;

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
    public void createTable(RecordSchema tableSchema, String tableName, int partition_num, int num_items) throws DatabaseException {
        storageManager.createTable(tableSchema, tableName, partition_num, num_items);
    }

    public abstract void InsertRecord(String table, TableRecord record, int partition_id) throws DatabaseException;

    /**
     * Used to implement fault tolerance
     */
    public abstract void asyncSnapshot(final long snapshotId, final int partitionId, final FTManager ftManager) throws IOException;

    public abstract void asyncCommit(final long groupId, final int partitionId, final FTManager ftManager) throws IOException;

    public abstract void syncReloadDB(SnapshotResult snapshotResult) throws IOException, ExecutionException, InterruptedException;

    public abstract void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException;
}