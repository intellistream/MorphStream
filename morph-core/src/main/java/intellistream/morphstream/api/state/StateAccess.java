package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes;

import java.util.HashMap;

public class StateAccess {
    private MetaTypes.AccessType accessType;
    private HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
    private HashMap<String, Object> conditionList; //Store all conditions required during txn-UDF
    public Object udfResult;
    public StateAccess(StateAccessDescription description) {
        stateObjectMap = new HashMap<>();
        accessType = description.getAccessType();
    }

    public void addStateObject(String stateObjName, StateObject stateObject) {
        stateObjectMap.put(stateObjName, stateObject);
    }
    public StateObject getStateObject(String stateObjName) {
        return stateObjectMap.get(stateObjName);
    }
    public HashMap<String, StateObject> getStateObjectMap() {
        return stateObjectMap;
    }
    public MetaTypes.AccessType getAccessType() {
        return accessType;
    }

}

