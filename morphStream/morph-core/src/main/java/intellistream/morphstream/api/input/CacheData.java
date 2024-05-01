package intellistream.morphstream.api.input;

public class CacheData {
    private final long timestamp; // Timestamp == 0 means normal CacheData, otherwise (-1) it indicates a stop signal
    private final int instanceID;
    private final int tupleID;
    private final int value;

    public CacheData(long timestamp, int instanceID, int tupleID, int value) {
        this.timestamp = timestamp;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }
    public int getInstanceID() {
        return instanceID;
    }
    public int getTupleID() {
        return tupleID;
    }
    public int getValue() {
        return value;
    }
}
