package intellistream.morphstream.engine.txn.durability.ftmanager;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.durability.struct.Result.persistResult;
import intellistream.morphstream.util.FaultToleranceConstants;

import java.io.IOException;

public abstract class FTManager extends Thread {
    public boolean running = true;

    public abstract void initialize(Configuration config) throws IOException;

    /**
     * @param snapshotId
     * @param path       input store path
     */
    public abstract boolean spoutRegister(long snapshotId, String path);

    public abstract persistResult spoutAskRecovery(int taskId, long snapshotOffset);

    public abstract long sinkAskLastTask(int taskId);

    public abstract boolean sinkRegister(long snapshot);

    /**
     * @param partitionId
     * @param status
     * @param Result
     */
    public abstract boolean boltRegister(int partitionId, FaultToleranceConstants.FaultToleranceStatus status, persistResult Result);

    public abstract void Listener() throws IOException;
}
