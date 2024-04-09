package intellistream.morphstream.api.input;

public class PatternData {
    private final int instanceID;
    private final int tupleID;
    private final boolean isWrite;

    public PatternData(int instanceID, int tupleID, boolean value) {
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.isWrite = value;
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
