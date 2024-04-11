package intellistream.morphstream.api.input;

public class CacheData {
    private final int instanceID;
    private final int tupleID;
    private final int value;

    public CacheData(int instanceID, int tupleID, int value) {
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
