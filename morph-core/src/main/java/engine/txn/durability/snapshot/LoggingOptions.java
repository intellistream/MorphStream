package engine.txn.durability.snapshot;

import util.FaultToleranceConstants;

public class LoggingOptions {
    private int parallelNum;
    private FaultToleranceConstants.CompressionType compressionAlg;
    private boolean isSelectiveLog = false;
    private int max_itr = 0;
    public LoggingOptions() {
        parallelNum = 1;
        compressionAlg = FaultToleranceConstants.CompressionType.None;
    }

    public LoggingOptions(int parallelNum, String compressionAlg) {
        this.parallelNum = parallelNum;
        switch(compressionAlg) {
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
    public LoggingOptions(int parallelNum, String compressionAlg, boolean isSelectiveLog, int max_itr) {
        this.parallelNum = parallelNum;
        switch(compressionAlg) {
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
        this.isSelectiveLog = isSelectiveLog;
        this.max_itr = max_itr;
    }
    public int getParallelNum() {
        return parallelNum;
    }
    public boolean isSelectiveLog() {
        return isSelectiveLog;
    }
    public int getMax_itr() {
        return max_itr;
    }
    public FaultToleranceConstants.CompressionType getCompressionAlg() {
        return compressionAlg;
    }
}
