package intellistream.morphstream.engine.stream.components.operators.api.spout;

import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.bolt.ft.MorphStreamBoltFT;
import intellistream.morphstream.engine.stream.components.operators.api.FaultTolerance;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.inputStore.InputDurabilityHelper;
import intellistream.morphstream.util.FaultToleranceConstants;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;


public abstract class AbstractSpoutFT extends AbstractSpout implements FaultTolerance {
    public int ftOption;
    public String inputStoreRootPath;
    public String inputStoreCurrentPath;
    public InputDurabilityHelper inputDurabilityHelper;
    public long remainTime = 0;

    public boolean isRecovery = false;
    public Queue<Object> recoveryInput = new ArrayDeque<>();
    public boolean arrivalControl;
    public FaultToleranceConstants.CompressionType compressionType;
    public int snapshot_interval;
    public FTManager ftManager;
    public FTManager loggingManager;
    protected AbstractSpoutFT(Logger log, int fid) {
        super(log, fid);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        ftOption = config.getInt("FTOption", 0);
        counter = 0;
        arrivalControl = config.getBoolean("arrivalControl");
        snapshot_interval = punctuation_interval * config.getInt("snapshotInterval");
        inputStoreRootPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("inputStore");
        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
        switch (config.getString("compressionAlg")) {
            case "None":
                this.compressionType = FaultToleranceConstants.CompressionType.None;
                break;
            case "XOR":// XOR GorillaV2
                this.compressionType = FaultToleranceConstants.CompressionType.XOR;
                break;
            case "Delta2Delta":// Delta-Delta GorillaV1
                this.compressionType = FaultToleranceConstants.CompressionType.Delta2Delta;
                break;
            case "Delta":// Delta DeltaBinary
                this.compressionType = FaultToleranceConstants.CompressionType.Delta;
                break;
            case "RLE":// Rle Rle
                this.compressionType = FaultToleranceConstants.CompressionType.RLE;
                break;
            case "Dictionary":// Dictionary
                this.compressionType = FaultToleranceConstants.CompressionType.Dictionary;
                break;
            case "Snappy":// Snappy Snappy
                this.compressionType = FaultToleranceConstants.CompressionType.Snappy;
                break;
            case "Zigzag":// Zigzag Zigzag
                this.compressionType = FaultToleranceConstants.CompressionType.Zigzag;
                break;
            case "Optimize":// Optimize Scabbard
                this.compressionType = FaultToleranceConstants.CompressionType.Optimize;
                break;
        }
        isRecovery = config.getBoolean("isRecovery");
        if (isRecovery) {
            remainTime = (long) (config.getInt("failureTime") * 1E6);
        }
        this.ftManager = this.getContext().getFtManager();
        this.loggingManager = this.getContext().getLoggingManager();
    }
    @Override
    public boolean input_reload(long snapshotOffset, long redoOffset) throws IOException, ExecutionException, InterruptedException {
        File file = new File(inputStoreRootPath + OsUtils.OS_wrapper(Long.toString(snapshotOffset)) + OsUtils.OS_wrapper(taskId + ".input"));
        if (file.exists()) {
            inputDurabilityHelper.reloadInput(file, recoveryInput, redoOffset, snapshotOffset, punctuation_interval);
            return true;
        } else {
            return false;
        }
    }
    @Override
    public boolean input_store(long currentOffset) throws IOException, ExecutionException, InterruptedException {
        this.inputDurabilityHelper.storeInput(this.inputQueue.toArray(), currentOffset, punctuation_interval, inputStoreCurrentPath);
        return true;
    }
    @Override
    public boolean snapshot(int counter) throws InterruptedException, BrokenBarrierException {
        return (counter % snapshot_interval == 0);
    }
}
