package intellistream.morphstream.api.input;

public class PatternData {
    private final int instanceID;
    private final int tupleID;
    private final boolean isWrite;

    public PatternData(int instanceID, int tupleID, boolean isWrite) {
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.isWrite = isWrite;
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
