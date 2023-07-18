package engine.txn.durability.logging.LoggingResult;

import engine.txn.durability.struct.Result.persistResult;

import java.io.Serializable;

public class LoggingResult implements Serializable, persistResult {
    public final String path;
    public final long groupId;
    public final int partitionId;
    public transient double size;// in KB


    public LoggingResult(long groupId, int partitionId, String path) {
        this.groupId = groupId;
        this.partitionId = partitionId;
        this.path = path;
    }
}
