package intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult;

import java.util.concurrent.ConcurrentHashMap;

import java.io.Serializable;

public class SnapshotCommitInformation implements Serializable {
    public final long snapshotId;
    public final ConcurrentHashMap<Integer, SnapshotResult> snapshotResults = new ConcurrentHashMap<>();
    public final String inputStorePath;

    public SnapshotCommitInformation(long snapshotId, String inputStorePath) {
        this.snapshotId = snapshotId;
        this.inputStorePath = inputStorePath;
    }
}


