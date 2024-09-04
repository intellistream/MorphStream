package intellistream.morphstream.transNFV.common;

public class SyncData {
    private final long timestamp;
    private final int tupleID;
    private final int value;

    public SyncData(long timestamp, int tupleID, int value) {
        this.timestamp = timestamp;
        this.tupleID = tupleID;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getTupleID() {
        return tupleID;
    }

    public int getValue() {
        return value;
    }
}
