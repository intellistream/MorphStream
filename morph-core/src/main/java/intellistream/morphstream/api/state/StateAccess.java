package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;

import java.util.Collection;
import java.util.HashMap;

public class StateAccess {
    private final String txnName; //name of txn that a stateAccess belongs to
    private final String stateAccessName;
    private String writeRecordName;
    private final MetaTypes.AccessType accessType;
    private final HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
    private final HashMap<String, Object> valueMap; //Store all values required during txn-UDF, including WRITE value
    public Object udfResult;

    public StateAccess(String txnName, StateAccessDescription description) {
        this.txnName = txnName;
        stateAccessName = description.getName();
        stateObjectMap = new HashMap<>();
        accessType = description.getAccessType();
        valueMap = new HashMap<>();
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
}
