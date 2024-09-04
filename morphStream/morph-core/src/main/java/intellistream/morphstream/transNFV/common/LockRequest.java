package intellistream.morphstream.transNFV.common;

public class LockRequest {
    public final long timestamp;
    public final boolean isWrite;

    public LockRequest(long timestamp, boolean isWrite) {
        this.timestamp = timestamp;
        this.isWrite = isWrite;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isWrite() {
        return isWrite;
    }
}
