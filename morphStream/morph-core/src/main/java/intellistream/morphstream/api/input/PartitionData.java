package intellistream.morphstream.api.input;

public class PartitionData {
    private final int instanceID;
    private final int tupleID;
    private final int value;

    public PartitionData(int instanceID, int tupleID, int value) {
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.value = value;
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
