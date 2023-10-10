package intellistream.morphstream.engine.txn.durability.ftmanager.ImplFTManager;

import intellistream.morphstream.common.io.LocalFS.FileSystem;
import intellistream.morphstream.common.io.LocalFS.LocalDataOutputStream;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.LoggingCommitInformation;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.LoggingResult;
import intellistream.morphstream.engine.txn.durability.recovery.RecoveryHelperProvider;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.struct.Result.persistResult;
import intellistream.morphstream.util.FaultToleranceConstants;
import intellistream.morphstream.util.FaultToleranceConstants.FaultToleranceStatus;
import intellistream.morphstream.util.OsUtils;
import intellistream.morphstream.util.Serialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PathManager extends FTManager {
    private final Logger LOG = LoggerFactory.getLogger(FTManager.class);
    private final ConcurrentHashMap<Long, List<FaultToleranceStatus>> callPersist = new ConcurrentHashMap<>();
    //Call if all tuples in this group committed
    private final ConcurrentHashMap<Long, List<FaultToleranceStatus>> callCommit = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, LoggingCommitInformation> registerCommit = new ConcurrentHashMap<>();
    private final Queue<Long> uncommittedId = new ConcurrentLinkedQueue<>();
    private final List<LoggingCommitInformation> LoggingCommitInformation = new Vector<>();
    private int parallelNum;
    private String walMetaPath;
    private String walPath;
    private long pendingId;
    /**
     * Used during recovery
     */
    private boolean isRecovery;
    private boolean isFailure;

    @Override
    public void initialize(Configuration config) throws IOException {
        this.parallelNum = config.getInt("parallelNum");
        walPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        walMetaPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("logging") + OsUtils.OS_wrapper("metaData.log");
        isRecovery = config.getBoolean("isRecovery");
        isFailure = config.getBoolean("isFailure");
        File walFile = new File(walPath);
        if (!walFile.exists()) {
            walFile.mkdirs();
        }
        if (isRecovery) {
            RecoveryHelperProvider.getCommittedLogMetaData(new File(walMetaPath), LoggingCommitInformation);
        }
        LOG.info("PathManager initialize successfully");
        this.setName("PathManager");
    }

    @Override
    public boolean spoutRegister(long groupId, String path) {
        if (this.registerCommit.containsKey(groupId)) {
            //TODO: if these are too many uncommitted group, notify the spout not to register
            LOG.info("groupID has been registered already");
            return false;
        } else {
            this.registerCommit.put(groupId, new LoggingCommitInformation(groupId));
            callPersist.put(groupId, initCall());
            callCommit.put(groupId, initCall());
            this.uncommittedId.add(groupId);
            LOG.info("Register group with offset: " + groupId + "; pending group: " + uncommittedId.size());
            return true;
        }
    }

    @Override
    public persistResult spoutAskRecovery(int taskId, long snapshotOffset) {
        RedoLogResult redoLogResult = new RedoLogResult();
        redoLogResult.threadId = taskId;
        for (LoggingCommitInformation loggingCommitInformation : LoggingCommitInformation) {
            if (loggingCommitInformation.groupId > snapshotOffset) {
                redoLogResult.addPath(loggingCommitInformation.loggingResults.get(taskId).path, loggingCommitInformation.groupId);
                redoLogResult.setLastedGroupId(loggingCommitInformation.groupId);
            }
        }
        return redoLogResult;
    }

    @Override
    public long sinkAskLastTask(int taskId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean sinkRegister(long snapshot) {
        return false;
    }

    @Override
    public boolean boltRegister(int partitionId, FaultToleranceStatus status, persistResult result) {
        LoggingResult loggingResult = (LoggingResult) result;
        if (status.equals(FaultToleranceConstants.FaultToleranceStatus.Persist)) {
            this.registerCommit.get(loggingResult.groupId).loggingResults.put(loggingResult.partitionId, loggingResult);
            this.callPersist.get(loggingResult.groupId).set(partitionId, status);
        } else if (status.equals(FaultToleranceConstants.FaultToleranceStatus.Commit)) {
            this.callCommit.get(loggingResult.groupId).set(partitionId, status);
        }

        return true;
    }

    @Override
    public void Listener() throws IOException {
        while (running) {
            if (all_register()) {
                LOG.info("PathManager received all register and commit log");
                logComplete(pendingId);
                if (uncommittedId.size() != 0) {
                    this.pendingId = uncommittedId.poll();
                } else {
                    this.pendingId = 0;
                }
                LOG.info("Pending commit: " + uncommittedId.size());
            }
        }
    }

    @Override
    public void run() {
        LOG.info("PathManager starts!");
        try {
            Listener();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (!isFailure) {
                File file = new File(this.walPath);
                FileSystem.deleteFile(file);
            }
            LOG.info("PathManager stops");
        }
    }

    private List<FaultToleranceConstants.FaultToleranceStatus> initCall() {
        List<FaultToleranceConstants.FaultToleranceStatus> statuses = new Vector<>();
        for (int i = 0; i < parallelNum; i++) {
            statuses.add(FaultToleranceConstants.FaultToleranceStatus.NULL);
        }
        return statuses;
    }

    private boolean all_register() {
        if (pendingId == 0) {
            if (uncommittedId.size() != 0) {
                pendingId = uncommittedId.poll();
                return !this.callPersist.get(pendingId).contains(FaultToleranceConstants.FaultToleranceStatus.NULL) && !this.callCommit.get(pendingId).contains(FaultToleranceConstants.FaultToleranceStatus.NULL);
            } else {
                return false;
            }
        } else {
            return !this.callPersist.get(pendingId).contains(FaultToleranceConstants.FaultToleranceStatus.NULL) && !this.callCommit.get(pendingId).contains(FaultToleranceConstants.FaultToleranceStatus.NULL);
        }
    }


    private void logComplete(long pendingId) throws IOException {
        LoggingCommitInformation loggingCommitInformation = this.registerCommit.get(pendingId);
        LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(new File(this.walMetaPath));
        DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
        byte[] result = Serialize.serializeObject(loggingCommitInformation);
        int length = result.length;
        dataOutputStream.writeInt(length);
        dataOutputStream.write(result);
        dataOutputStream.close();
        this.registerCommit.remove(pendingId);
        LOG.info("PathManager commit the Path to the current.log");
    }
}
