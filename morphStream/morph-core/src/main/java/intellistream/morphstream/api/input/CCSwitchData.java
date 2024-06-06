package intellistream.morphstream.api.input;

public class CCSwitchData {
    private final int tupleID;
    private final int oldCC;
    private final int newCC;

    public CCSwitchData(int tupleID, int oldCC, int newCC) {
        this.tupleID = tupleID;
        this.oldCC = oldCC;
        this.newCC = newCC;
    }

    public int getTupleID() {
        return tupleID;
    }
    public int getOldCC() {
        return oldCC;
    }
    public int getNewCC() {
        return newCC;
    }
}
