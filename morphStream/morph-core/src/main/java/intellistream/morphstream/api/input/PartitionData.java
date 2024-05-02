package intellistream.morphstream.api.input;

import java.util.concurrent.BlockingQueue;

public class PartitionData {
    private final long timeStamp;
    private final long txnReqId;
    private final int instanceID;
    private final int tupleID;
    private final int value;
    private final BlockingQueue<Integer> senderResponseQueue;

    public PartitionData(long timeStamp, long txnReqId, int instanceID, int tupleID, int value, BlockingQueue<Integer> senderResponseQueue) {
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.value = value;
        this.senderResponseQueue = senderResponseQueue;
    }

    public PartitionData(long timeStamp, long txnReqId, int instanceID, int tupleID, int value) {
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.value = value;
        this.senderResponseQueue = null;
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
    public BlockingQueue<Integer> getSenderResponseQueue() {
        return senderResponseQueue;
    }
}
