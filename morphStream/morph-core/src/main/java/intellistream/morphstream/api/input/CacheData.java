package intellistream.morphstream.api.input;

import java.util.concurrent.BlockingQueue;

public class CacheData {
    private final long txnReqID;
    private final long timestamp; // Timestamp == 0 means normal CacheData, otherwise (-1) it indicates a stop signal
    private final int instanceID;
    private final int tupleID;
    private final int value;
    private final int puncID;
    private final BlockingQueue<Integer> senderResponseQueue; //For efficient response handling through CacheData itself

    public CacheData(long txnReqID, long timestamp, int instanceID, int tupleID, int value, int puncID) {
        this.txnReqID = txnReqID;
        this.timestamp = timestamp;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.value = value;
        this.puncID = puncID;
        this.senderResponseQueue = null;
    }

    public CacheData(long txnReqID, long timestamp, int instanceID, int tupleID, int value, int puncID, BlockingQueue<Integer> senderResponseQueue) {
        this.txnReqID = txnReqID;
        this.timestamp = timestamp;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.value = value;
        this.puncID = puncID;
        this.senderResponseQueue = senderResponseQueue;
    }

    public long getTimestamp() {
        return timestamp;
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
    public long getTxnReqID() {
        return txnReqID;
    }
    public int getPuncID() {
        return puncID;
    }
    public BlockingQueue<Integer> getSenderResponseQueue() {
        return senderResponseQueue;
    }
}
