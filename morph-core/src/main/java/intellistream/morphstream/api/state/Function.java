package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class Function {
    @Getter
    private final String operationID; //bid + stateAccessIndex
    @Getter
    private final String operatorID; //name of app that a txn belongs to
    @Getter
    private final String DAGName; //name of txn that a stateAccess belongs to
    @Getter
    private final String functionName;
    @Getter
    private final List<String> fatherStateAccessNames;
    @Setter @Getter
    private String writeRecordName;
    @Getter
    private final MetaTypes.AccessType accessType;
    private final HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
    @Getter
    private final HashMap<String, Object> paraMap; //Store all values required during txn-UDF, including WRITE value
    public Object udfResult;
    private boolean isAborted;

    public Function(String operationID, String operatorID, String DAGName, FunctionDescription description) {
        this.operationID = operationID;
        this.operatorID = operatorID;
        this.DAGName = DAGName;
        functionName = description.getName();
        fatherStateAccessNames = description.getFatherNames();
        stateObjectMap = new HashMap<>();
        accessType = description.getAccessType();
        paraMap = new HashMap<>();
        isAborted = false;
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

    public void addPara(String valueName, Object value) {
        paraMap.put(valueName, value);
    }
    public Object getPara(String valueName) {
        return paraMap.get(valueName);
    }

    public void setAborted() {
        isAborted = true;
    }

    public boolean isAborted() {
        return isAborted;
    }
}

