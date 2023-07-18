package engine.txn.durability.logging.LoggingResult;

import engine.txn.durability.ftmanager.FTManager;

import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;

public class Attachment {
    public Path path;
    public long groupId;
    public int partitionId;
    public AsynchronousFileChannel asyncChannel;
    public FTManager ftManager;

    public Attachment(Path path, long snapshotId, int partitionId, AsynchronousFileChannel asyncChannel, FTManager ftManager) {
        this.asyncChannel = asyncChannel;
        this.groupId = snapshotId;
        this.partitionId = partitionId;
        this.path = path;
        this.ftManager = ftManager;
    }
    public LoggingResult getLoggingResult() {
        return new LoggingResult(this.groupId, this.partitionId, this.path.toString());
    }
}
