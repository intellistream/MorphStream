package intellistream.morphstream.transNFV.common;

public class PatternData {
    private final int instanceID;
    private final int tupleID;
    private final String type;

    public PatternData(int instanceID, int tupleID, String type) {
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.type = type;
    }

    public int getInstanceID() {
        return instanceID;
    }
    public int getTupleID() {
        return tupleID;
    }
    public String getType() {
        return type;
    }
}
