package communication.dao;

import java.util.concurrent.BlockingQueue;

public class VNFRequest {
    private int reqID;
    private int instanceID;
    private int tupleID;
    private int type; // 0: read, 1: write, 2: read-write
    private long createTime; // Time when the request is created by the instance
    private long finishTime; // Time when the finished request is received by the instance
    private int instancePuncID; // Punctuation ID of the request
    private int value;
    private int saID;
    private int logicalTS; // For offloading central locks
    private BlockingQueue<Integer> txnACKQueue;

    public VNFRequest(int reqID, int instanceID, int tupleID, int type, long createTime, int instancePuncID, int value, int saID) {
        this.reqID = reqID;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.type = type;
        this.createTime = createTime;
        this.instancePuncID = instancePuncID;
        this.value = value;
        this.saID = saID;
    }

    public VNFRequest(int reqID, int instanceID, int tupleID, int type, long createTime, int instancePuncID, int value, int saID, BlockingQueue<Integer> txnACKQueue) {
        this.reqID = reqID;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.type = type;
        this.createTime = createTime;
        this.instancePuncID = instancePuncID;
        this.value = value;
        this.saID = saID;
        this.txnACKQueue = txnACKQueue;
    }

    //reqID, tupleID, instanceID, value, saID, type, instancePuncID, pktStartTime, pktEndTime, (responseQueue)
    public int getReqID() {
        return reqID;
    }
    public int getInstanceID() {
        return instanceID;
    }
    public int getTupleID() {
        return tupleID;
    }
    public int getType() {
        return type;
    }
    public long getCreateTime() {
        return createTime;
    }
    public long getFinishTime() {
        return finishTime;
    }
    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }
    public int getInstancePuncID() {
        return instancePuncID;
    }
    public int getValue() {
        return value;
    }
    public int getSaID() {
        return saID;
    }
    public int getLogicalTS() {
        return logicalTS;
    }

    public void setLogicalTS(int logicalTS) {
        this.logicalTS = logicalTS;
    }

    public BlockingQueue<Integer> getTxnACKQueue() {
        return txnACKQueue;
    }
}
