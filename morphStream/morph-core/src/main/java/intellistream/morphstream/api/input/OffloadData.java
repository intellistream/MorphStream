package intellistream.morphstream.api.input;

import java.util.concurrent.BlockingQueue;

public class OffloadData {
    private final int instanceID;
    private final long timeStamp;
    private final long txnReqId;
    private final int tupleID;
    private final int txnIndex;
    private final int saIndex;
    private final int isAbort;
    private final int saType;
    private int logicalTimeStamp;
    private final BlockingQueue<Integer> senderResponseQueue;

    public OffloadData(long timeStamp, int instanceID, long txnReqId, int tupleID, int txnIndex, int saIndex, int isAbort, int saType) {
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
        this.txnIndex = txnIndex;
        this.saIndex = saIndex;
        this.isAbort = isAbort;
        this.saType = saType;
        this.senderResponseQueue = null;
    }

    public OffloadData(long timeStamp, int instanceID, long txnReqId, int tupleID, int txnIndex, int saIndex, int isAbort, int saType, BlockingQueue<Integer> senderResponseQueue) {
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.timeStamp = timeStamp;
        this.txnReqId = txnReqId;
        this.txnIndex = txnIndex;
        this.saIndex = saIndex;
        this.isAbort = isAbort;
        this.saType = saType;
        this.senderResponseQueue = senderResponseQueue;
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

    public BlockingQueue<Integer> getSenderResponseQueue() {
        return senderResponseQueue;
    }
    public int getLogicalTimeStamp() {
        return logicalTimeStamp;
    }
    public void setLogicalTimeStamp(int logicalTimeStamp) {
        this.logicalTimeStamp = logicalTimeStamp;
    }
}
