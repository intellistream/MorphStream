package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;

import java.util.Collection;
import java.util.HashMap;

public class StateAccess {
    private final String operationID; //bid + stateAccessIndex
    private final String operatorID; //name of app that a txn belongs to
    private final String txnName; //name of txn that a stateAccess belongs to
    private final String stateAccessName;
    private String writeRecordName;
    private final MetaTypes.AccessType accessType;
    private final HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
    private final HashMap<String, Object> valueMap; //Store all values required during txn-UDF, including WRITE value
    public Object udfResult;
    private boolean isAborted;

    public StateAccess(String operationID, String operatorID, String txnName, StateAccessDescription description) {
        this.operationID = operationID;
        this.operatorID = operatorID;
        this.txnName = txnName;
        stateAccessName = description.getName();
        stateObjectMap = new HashMap<>();
        accessType = description.getAccessType();
        valueMap = new HashMap<>();
        isAborted = false;
    }

    public String getOperationID() {
        return operationID;
    }

    public String getOperatorID() {
        return operatorID;
    }

    public String getTxnName() {
        return txnName;
    }

    public String getStateAccessName() {
        return stateAccessName;
    }

    public MetaTypes.AccessType getAccessType() {
        return accessType;
    }

    public void setWriteRecordName(String writeRecordName) {
        this.writeRecordName = writeRecordName;
    }

    public void setUpdatedStateObject(SchemaRecord updatedWriteRecord) {
        stateObjectMap.get(writeRecordName).setSchemaRecord(updatedWriteRecord);
    }

    public StateObject getStateObjectToWrite() {
        return stateObjectMap.get(writeRecordName);
    }

    public void addStateObject(String stateObjName, StateObject stateObject) {
        stateObjectMap.put(stateObjName, stateObject);
    }
    public StateObject getStateObject(String stateObjName) {
        return stateObjectMap.get(stateObjName);
    }
    public Collection<StateObject> getStateObjects() {
        return stateObjectMap.values();
    }

    public void addValue(String valueName, Object value) {
        valueMap.put(valueName, value);
    }
    public Object getValue(String valueName) {
        return valueMap.get(valueName);
    }

    public HashMap<String, Object> getValueMap() {
        return valueMap;
    }
    public void setAborted() {
        isAborted = true;
    }

    public boolean isAborted() {
        return isAborted;
    }
}

