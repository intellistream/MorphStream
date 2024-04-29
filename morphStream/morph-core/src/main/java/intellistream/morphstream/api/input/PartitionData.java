package intellistream.morphstream.api.input;

public class PartitionData {
    private final long timeStamp;
    private final long txnReqId;
    private final int instanceID;
    private final int tupleID;
    private final int value;

    public PartitionData(long timeStamp, long txnReqId, int instanceID, int tupleID, int value) {
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
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

    public long getTimeStamp() {
        return timeStamp;
    }

    public long getTxnReqId() {
        return txnReqId;
    }
}
