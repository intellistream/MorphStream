package intellistream.morphstream.transNFV.common;

public class SyncData {
    private final int tupleID;
    private final int value;

    public SyncData(int tupleID, int value) {
        this.tupleID = tupleID;
        this.value = value;
    }

    public int getTupleID() {
        return tupleID;
    }

    public int getValue() {
        return value;
    }
}
