package engine.txn.durability.logging.LoggingResult;

import util.ConcurrentHashMap;

import java.io.Serializable;

public class LoggingCommitInformation implements Serializable {
    public final long groupId;
    public final ConcurrentHashMap<Integer, LoggingResult> loggingResults = new ConcurrentHashMap<>();

    public LoggingCommitInformation(long groupId) {
        this.groupId = groupId;
    }
}
