package intellistream.morphstream.api.input;

public class OffloadData {
    private final int instanceID;
    private final long timeStamp;
    private final long txnReqId;
    private final int tupleID;
    private final int txnIndex;
    private final int saIndex;
    private final int isAbort;
    private final int saType;

    public OffloadData(long timeStamp, int instanceID, long txnReqId, int tupleID, int txnIndex, int saIndex, int isAbort, int saType) {
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
        this.txnIndex = txnIndex;
        this.saIndex = saIndex;
        this.isAbort = isAbort;
        this.saType = saType;
    }

    public int getInstanceID() {
        return instanceID;
    }

    public int getTupleID() {
        return tupleID;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public long getTxnReqId() {
        return txnReqId;
    }

    public int getTxnIndex() {
        return txnIndex;
    }

    public int getSaIndex() {
        return saIndex;
    }

    public int getIsAbort() {
        return isAbort;
    }

    public int getSaType() {
        return saType;
    }
}
