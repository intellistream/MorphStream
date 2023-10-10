package intellistream.morphstream.engine.txn.durability.snapshot;

import intellistream.morphstream.util.FaultToleranceConstants;

public class SnapshotOptions {
    private final int parallelNum;
    private FaultToleranceConstants.CompressionType compressionAlg;

    public SnapshotOptions() {
        parallelNum = 1;
        compressionAlg = FaultToleranceConstants.CompressionType.None;
    }

    public SnapshotOptions(int parallelNum, String compressionAlg) {
        this.parallelNum = parallelNum;
        switch (compressionAlg) {
            case "None":
                this.compressionAlg = FaultToleranceConstants.CompressionType.None;
                break;
            case "Snappy":
                this.compressionAlg = FaultToleranceConstants.CompressionType.Snappy;
                break;
            case "XOR":
                this.compressionAlg = FaultToleranceConstants.CompressionType.XOR;
                break;
            case "RLE":
                this.compressionAlg = FaultToleranceConstants.CompressionType.RLE;
                break;
        }
    }

    public int getParallelNum() {
        return parallelNum;
    }

    public FaultToleranceConstants.CompressionType getCompressionAlg() {
        return compressionAlg;
    }
}
