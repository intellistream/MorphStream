package intellistream.morphstream.util.datatypes;

import java.util.HashMap;

public class TxnMetaData {
    private int instanceID;
    private HashMap<String, Integer> operationMap; //Each entry represents one operation, maps target (state tuple ID) to operation type (R:0/W:1)
    public TxnMetaData(int instanceID, HashMap<String, Integer> operationMap) {
        this.instanceID = instanceID;
        this.operationMap = operationMap;
    }
    public int getInstanceID() {
        return instanceID;
    }
    public HashMap<String, Integer> getOperationMap() {
        return operationMap;
    }
}
