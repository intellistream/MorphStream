package intellistream.morphstream.engine.db.storage;

import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.db.storage.table.BaseTable;
import intellistream.morphstream.engine.db.storage.table.RecordSchema;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public abstract class StorageManager {
    public abstract void close() throws IOException;

    public abstract void dropAllTables() throws IOException;

    public abstract void createTable(RecordSchema tableSchema, String tableName, int partitionNum, int numItems) throws DatabaseException;

    public abstract void InsertRecord(String table, TableRecord record, int partitionId) throws DatabaseException;

    public abstract void asyncSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws IOException;

    public abstract void syncReloadDatabase(SnapshotResult snapshotResult) throws IOException, ExecutionException, InterruptedException;
    public Map<String, BaseTable> getTables() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
    public BaseTable getTable(String tableName) throws DatabaseException {throw new UnsupportedOperationException("Not implemented yet");}
}
