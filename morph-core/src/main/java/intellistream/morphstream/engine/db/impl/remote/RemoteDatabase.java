package intellistream.morphstream.engine.db.impl.remote;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.db.Database;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.storage.impl.RemoteStorageManager;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class RemoteDatabase extends Database {
    private final RemoteCallLibrary remoteCallLibrary = new RemoteCallLibrary();
    public RemoteDatabase(Configuration configuration) {
        this.storageManager = new RemoteStorageManager(MorphStreamEnv.get().rdmaWorkerManager().getCacheBuffer(), configuration.getInt("workerNum"), configuration.getInt("tthread"));
    }

    @Override
    public void InsertRecord(String table, TableRecord record, int partition_id) throws DatabaseException {

    }

    @Override
    public void asyncSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void asyncCommit(long groupId, int partitionId, FTManager ftManager) throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void syncReloadDB(SnapshotResult snapshotResult) throws IOException, ExecutionException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
    public static void main(String[] args) {
        RemoteCallLibrary callLibrary = new RemoteCallLibrary();
        callLibrary.init();
        callLibrary.connect();
        callLibrary.read("table1", "key1");
        callLibrary.write("table1","key1", 1);
    }
}
