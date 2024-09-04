package intellistream.morphstream.transNFV.common;

import java.util.concurrent.BlockingQueue;

public class PartitionData {
    private final long timeStamp;
    private final long txnReqId;
    private final int instanceID;
    private final int tupleID;
    private final int value;
    private final int saIndex;
    private final int puncID;
    private final BlockingQueue<Integer> senderResponseQueue;

    public PartitionData(long timeStamp, long txnReqId, int instanceID, int tupleID, int value, int saIndex, int puncID, BlockingQueue<Integer> senderResponseQueue) {
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.value = value;
        this.saIndex = saIndex;
        this.puncID = puncID;
        this.senderResponseQueue = senderResponseQueue;
    }

    public PartitionData(long timeStamp, long txnReqId, int instanceID, int tupleID, int value, int saIndex, int puncID) {
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.value = value;
        this.saIndex = saIndex;
        this.puncID = puncID;
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
    public int getPuncID() {
        return puncID;
    }
    public int getSaIndex() {
        return saIndex;
    }
    public BlockingQueue<Integer> getSenderResponseQueue() {
        return senderResponseQueue;
    }
}
