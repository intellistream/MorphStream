package intellistream.morphstream.transNFV.data;

public class Operation {
    private final int key;
    private final int value;
    private final long timestamp;
    private final boolean isWrite;

    public Operation(int key, int value, long timestamp, boolean isWrite) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.isWrite = isWrite;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isWrite() {
        return isWrite;
    }
}
