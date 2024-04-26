package communication.dao;

public class VNFRequest {
//    (reqID, instanceID, tupleID, type)
    private int reqID;
    private int instanceID;
    private int tupleID;
    private int type;
    private long createTime;
    private long finishTime;

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
