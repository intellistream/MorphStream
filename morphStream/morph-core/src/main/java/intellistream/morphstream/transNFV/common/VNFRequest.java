package intellistream.morphstream.transNFV.common;

import java.util.concurrent.BlockingQueue;

public class VNFRequest {
    private final int reqID;
    private final int instanceID;
    private final int tupleID;
    private final int value;
    private final String type; // Read or Write
    private final String scope; // 0: per-flow, 1: cross-flow
    private final long createTime; // Time when the request is created by the instance
    private long finishTime; // Time when the finished request is received by the instance
    private final int instancePuncID; // Punctuation ID of the request
    private final int vnfID;
    private final int saID;
    private boolean chcEnableLocalExecution = false;

    public VNFRequest(int reqID, int instanceID, int key, int value, String scope, String type, int vnfID, int saID, long createTime, int instancePuncID) {
        this.reqID = reqID;
        this.instanceID = instanceID;
        this.tupleID = key;
        this.type = type;
        this.scope = scope;
        this.createTime = createTime;
        this.instancePuncID = instancePuncID;
        this.value = value;
        this.vnfID = vnfID;
        this.saID = saID;
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
    public String getType() {
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
    public int getVnfID() {
        return vnfID;
    }
    public String getScope() {
        return scope;
    }
    public void enableCHCLocalExecution() {
        chcEnableLocalExecution = true;
    }
    public boolean proceedCHCLocalExecution() {
        return chcEnableLocalExecution;
    }

}
