package intellistream.morphstream.transNFV.data;

public class PatternData {
    private final int instanceID;
    private final int tupleID;
    private final int type;

    public PatternData(int instanceID, int tupleID, int type) {
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
    public int getType() {
        return type;
    }
}
