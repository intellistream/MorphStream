package intellistream.morphstream.engine.db.impl;

import intellistream.morphstream.engine.db.Database;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class RemoteDatabase extends Database {

    @Override
    public void InsertRecord(String table, TableRecord record, int partition_id) throws DatabaseException {

    }

    @Override
    public void asyncSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws IOException {

    }

    @Override
    public void asyncCommit(long groupId, int partitionId, FTManager ftManager) throws IOException {

    }

    @Override
    public void syncReloadDB(SnapshotResult snapshotResult) throws IOException, ExecutionException, InterruptedException {

    }

    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {

    }
}
