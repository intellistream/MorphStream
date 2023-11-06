package intellistream.morphstream.engine.txn.db;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.CommandLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.DependencyLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.LSNVectorLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.PathLoggingManager;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import intellistream.morphstream.engine.txn.storage.EventManager;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * original designer for CavaliaDatabase: Yingjun Wu.
 */
public class CavaliaDatabase extends Database {
    public CavaliaDatabase(Configuration configuration) {
        storageManager = new StorageManager(configuration);
        eventManager = new EventManager();
        switch (configuration.getInt("FTOption")) {
            case 0:
            case 1:
                this.loggingManager = null;
                break;
            case 3:
                this.loggingManager = new PathLoggingManager(configuration);
                break;
            case 4:
                this.loggingManager = new LSNVectorLoggingManager(this.storageManager.tables, configuration);
                break;
            case 5:
                this.loggingManager = new DependencyLoggingManager(this.storageManager.tables, configuration);
                break;
            case 6:
                this.loggingManager = new CommandLoggingManager(this.storageManager.tables, configuration);
                break;
            default:
                throw new UnsupportedOperationException("No such kind of FTOption");
        }
    }

    /**
     * @param table
     * @param record
     * @throws DatabaseException
     */
    @Override
    public void InsertRecord(String table, TableRecord record, int partition_id) throws DatabaseException {
        storageManager.InsertRecord(table, record, partition_id);
    }

    @Override
    public void asyncSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws IOException {
        this.storageManager.asyncSnapshot(snapshotId, partitionId, ftManager);
    }

    @Override
    public void syncReloadDB(SnapshotResult snapshotResult) throws IOException, ExecutionException, InterruptedException {
        this.storageManager.syncReloadDatabase(snapshotResult);
    }

    @Override
    public void asyncCommit(long groupId, int partitionId, FTManager ftManager) throws IOException {
        this.loggingManager.commitLog(groupId, partitionId, ftManager);
    }

    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {
        this.loggingManager.syncRetrieveLogs(redoLogResult);
    }
}
