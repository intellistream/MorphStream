package intellistream.morphstream.api.input;

public class PatternData {
    private final long timeStamp;
    private final int instanceID;
    private final int tupleID;
    private final boolean isWrite;

    public PatternData(long timeStamp, int instanceID, int tupleID, boolean isWrite) {
        this.timeStamp = timeStamp;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.isWrite = isWrite;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
    public int getInstanceID() {
        return instanceID;
    }
    public int getTupleID() {
        return tupleID;
    }
    public boolean getIsWrite() {
        return isWrite;
    }
}
