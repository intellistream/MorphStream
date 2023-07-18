package engine.txn.durability.ftmanager.ImplFTManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.io.LocalFS.FileSystem;
import common.io.LocalFS.LocalDataOutputStream;
import util.tools.Serialize;
import engine.txn.durability.ftmanager.FTManager;
import engine.txn.durability.recovery.RecoveryHelperProvider;
import engine.txn.durability.snapshot.SnapshotResult.SnapshotCommitInformation;
import engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import engine.txn.durability.struct.Result.persistResult;
import util.FaultToleranceConstants.FaultToleranceStatus;
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

public class CheckpointManager extends FTManager {
    private final Logger LOG = LoggerFactory.getLogger(CheckpointManager.class);
    private int parallelNum;
    //Call if state snapshot complete
    private ConcurrentHashMap<Long, List<FaultToleranceStatus>> callSnapshot = new ConcurrentHashMap<>();
    //Call if all tuples in this epoch committed
    private ConcurrentHashMap<Long, List<FaultToleranceStatus>> callCommit = new ConcurrentHashMap<>();
    //<snapshotId, SnapshotCommitInformation>
    private ConcurrentHashMap<Long, SnapshotCommitInformation> registerSnapshot = new ConcurrentHashMap<>();
    private String metaPath;
    private String basePath;
    private String inputStoreRootPath;
    private String outputStoreRootPath;
    private long lastTask[];
    private Queue<Long> uncommittedId = new ConcurrentLinkedQueue<>();
    private long pendingSnapshotId;
    /** Used during recovery */
    private boolean isRecovery;
    private boolean isFailure;
    private SnapshotCommitInformation latestSnapshotCommitInformation;
    @Override
    public void initialize(Configuration config) throws IOException {
        this.parallelNum = config.getInt("parallelNum");
        basePath = config.getString("rootFilePath") + OsUtils.OS_wrapper("snapshot");
        metaPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("snapshot") + OsUtils.OS_wrapper("metaData.log");
        inputStoreRootPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("inputStore");
        outputStoreRootPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("outputStore");
        isRecovery = config.getBoolean("isRecovery");
        isFailure = config.getBoolean("isFailure");
        lastTask = new long[parallelNum];
        File file = new File(this.basePath);
        if (!file.exists()) {
            file.mkdirs();
        }
        if (isRecovery) {
            latestSnapshotCommitInformation = RecoveryHelperProvider.getLatestCommitSnapshotCommitInformation(new File(metaPath));
            RecoveryHelperProvider.getLastTask(lastTask, outputStoreRootPath);
        } else {
            SnapshotCommitInformation snapshotCommitInformation = new SnapshotCommitInformation(0L, config.getString("rootFilePath") + OsUtils.OS_wrapper("inputStore") + OsUtils.OS_wrapper("inputStore"));
            byte[] result = Serialize.serializeObject(snapshotCommitInformation);
            LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(new File(this.metaPath));
            DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
            int length = result.length;
            dataOutputStream.writeInt(length);
            dataOutputStream.write(result);
            dataOutputStream.close();
        }
        this.setName("CheckpointManager");
        LOG.info("CheckpointManager initialize successfully");
    }

    @Override
    public boolean spoutRegister(long snapshotId, String path) {
        if (this.registerSnapshot.containsKey(snapshotId)) {
            //TODO: if these are too many uncommitted snapshot, notify the spout not to register
            LOG.info("SnapshotID has been registered already");
            return false;
        } else {
            this.registerSnapshot.put(snapshotId, new SnapshotCommitInformation(snapshotId, path));
            callSnapshot.put(snapshotId, initCall());
            callCommit.put(snapshotId, initCall());
            this.uncommittedId.add(snapshotId);
            LOG.info("Register snapshot with offset: " + snapshotId + "; pending snapshot: " + uncommittedId.size());
            return true;
        }
    }

    @Override
    public persistResult spoutAskRecovery(int taskId, long snapshotOffset) {
        return null;
//        return latestSnapshotCommitInformation.snapshotResults.get(taskId);
    }

    @Override
    public long sinkAskLastTask(int taskId) {
        return this.lastTask[taskId];
    }

    @Override
    public boolean sinkRegister(long snapshot) {
        return false;
    }

    @Override
    public boolean boltRegister(int partitionId, FaultToleranceStatus status, persistResult result) {
        SnapshotResult snapshotResult = (SnapshotResult) result;
        if (status.equals(FaultToleranceStatus.Snapshot)) {
            this.registerSnapshot.get(snapshotResult.snapshotId).snapshotResults.put(snapshotResult.partitionId, snapshotResult);
            this.callSnapshot.get(snapshotResult.snapshotId).set(partitionId, status);
        } else if (status.equals(FaultToleranceStatus.Commit)) {
            this.callCommit.get(((SnapshotResult) result).snapshotId).set(partitionId, status);
        }
        return true;
    }

    @Override
    public void Listener() throws IOException {
        while (running) {
            if (all_register()) {
                LOG.info("CheckpointManager received all register and commit snapshot");
                snapshotComplete(pendingSnapshotId);
                if (uncommittedId.size() != 0) {
                    this.pendingSnapshotId = uncommittedId.poll();
                } else {
                    this.pendingSnapshotId = 0;
                }
                LOG.info("Pending snapshot: " + uncommittedId.size());
            }
        }
    }

    @Override
    public void run() {
        LOG.info("CheckpointManager starts!");
        try {
            Listener();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (!isFailure) {
                File file = new File(this.basePath);
                FileSystem.deleteFile(file);
                file = new File(this.inputStoreRootPath);
                FileSystem.deleteFile(file);
                file = new File(this.outputStoreRootPath);
                FileSystem.deleteFile(file);
            }
            LOG.info("CheckpointManager stops");
        }
    }

    private List<FaultToleranceStatus> initCall() {
        List<FaultToleranceStatus> statuses = new Vector<>();
        for (int i = 0; i < parallelNum; i++) {
            statuses.add(FaultToleranceStatus.NULL);
        }
        return statuses;
    }
    private boolean all_register() {
        if (pendingSnapshotId == 0) {
            if (uncommittedId.size() != 0) {
                pendingSnapshotId = uncommittedId.poll();
                return !this.callSnapshot.get(pendingSnapshotId).contains(FaultToleranceStatus.NULL) && !this.callCommit.get(pendingSnapshotId).contains(FaultToleranceStatus.NULL);
            } else {
                return false;
            }
        } else {
            return !this.callSnapshot.get(pendingSnapshotId).contains(FaultToleranceStatus.NULL) && !this.callCommit.get(pendingSnapshotId).contains(FaultToleranceStatus.NULL);
        }
    }
    private void snapshotComplete(long snapshotId) throws IOException {
        SnapshotCommitInformation snapshotCommitInformation = this.registerSnapshot.get(snapshotId);
        LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(new File(this.metaPath));
        DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
        byte[] result = Serialize.serializeObject(snapshotCommitInformation);
        int length = result.length;
        dataOutputStream.writeInt(length);
        dataOutputStream.write(result);
        dataOutputStream.close();
        this.registerSnapshot.remove(snapshotId);
        LOG.info("CheckpointManager commit the snapshot to the current.log");
    }
}
