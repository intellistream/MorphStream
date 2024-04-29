package communication.dao;

public class VNFRequest {
    private int reqID;
    private int instanceID;
    private int tupleID;
    private int type; // 0: read, 1: write, 2: read-write
    private long createTime; // Time when the request is created by the instance
    private long finishTime; // Time when the finished request is received by the instance

    public VNFRequest(int reqID, int instanceID, int tupleID, int type, long createTime) {
        this.reqID = reqID;
        this.instanceID = instanceID;
        this.tupleID = tupleID;
        this.type = type;
        this.createTime = createTime;
    }

    public int getReqID() {
        return reqID;
    }
    public void setReqID(int reqID) {
        this.reqID = reqID;
    }
    public int getInstanceID() {
        return instanceID;
    }
    public void setInstanceID(int instanceID) {
        this.instanceID = instanceID;
    }
    public int getTupleID() {
        return tupleID;
    }
    public void setTupleID(int tupleID) {
        this.tupleID = tupleID;
    }
    public int getType() {
        return type;
    }
    public void setType(int type) {
        this.type = type;
    }
    public long getCreateTime() {
        return createTime;
    }
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    public long getFinishTime() {
        return finishTime;
    }
    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }
}
