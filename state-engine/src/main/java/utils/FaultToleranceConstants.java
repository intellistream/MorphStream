package utils;

public class FaultToleranceConstants {
    public enum FaultToleranceStatus {
        NULL,Undo,Persist,Commit,Snapshot
    }
    public enum CompressionType {
        None, XOR, Delta2Delta, Delta, RLE, Dictionary, Snappy, Zigzag, Optimize
    }
}
