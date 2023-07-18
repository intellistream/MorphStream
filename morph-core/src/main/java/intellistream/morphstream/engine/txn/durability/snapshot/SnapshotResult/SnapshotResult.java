package intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult;

import intellistream.morphstream.engine.txn.durability.struct.Result.persistResult;

import java.io.Serializable;

public class SnapshotResult implements Serializable, persistResult {
    public final String path;
    public final long snapshotId;
    public final int partitionId;
    public transient double size;//in KB


    public SnapshotResult(long snapshotId, int partitionId, String path) {
        this.snapshotId = snapshotId;
        this.partitionId = partitionId;
        this.path = path;
    }
}
