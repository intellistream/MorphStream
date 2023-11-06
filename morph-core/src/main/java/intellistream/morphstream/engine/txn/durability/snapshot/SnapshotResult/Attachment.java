package intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult;

import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;

import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;

public class Attachment {
    public Path path;
    public long snapshotId;
    public int partitionId;
    public AsynchronousFileChannel asyncChannel;
    public FTManager ftManager;

    public Attachment(Path path, long snapshotId, int partitionId, AsynchronousFileChannel asyncChannel, FTManager ftManager) {
        this.asyncChannel = asyncChannel;
        this.snapshotId = snapshotId;
        this.partitionId = partitionId;
        this.path = path;
        this.ftManager = ftManager;
    }

    public SnapshotResult getSnapshotResult() {
        return new SnapshotResult(this.snapshotId, this.partitionId, this.path.toString());
    }
}