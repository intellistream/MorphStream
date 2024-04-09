package intellistream.morphstream.api.input;

public class OffloadData {
    private final int instanceID;
    private final long timeStamp;
    private final long txnReqId;
    private final int tupleID;
    private final int txnIndex;
    private final int saIndex;
    private final int isAbort;

    //target = 3 (int) +0
//timeStamp(long) + 1
//txnReqId(long) + 2
//tupleID (int) + 3
//txnIndex(int) + 4
//saIndex(int) + 5
//isAbort(int);6

    public OffloadData(int instanceID, long timeStamp, long txnReqId, int tupleID, int txnIndex, int saIndex, int isAbort) {
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
        this.txnIndex = txnIndex;
        this.saIndex = saIndex;
        this.isAbort = isAbort;
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

}
