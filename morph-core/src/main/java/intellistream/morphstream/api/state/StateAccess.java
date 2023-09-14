package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes;

import java.util.Collection;
import java.util.HashMap;

public class StateAccess {
    private final MetaTypes.AccessType accessType;
    private final HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
    private final HashMap<String, Object> conditionList; //Store all conditions required during txn-UDF
    public Object udfResult;
    public StateAccess(StateAccessDescription description) {
        stateObjectMap = new HashMap<>();
        accessType = description.getAccessType();
        conditionList = new HashMap<>();
    }

    public MetaTypes.AccessType getAccessType() {
        return accessType;
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

    public void addCondition(String conditionName, Object condition) {
        conditionList.put(conditionName, condition);
    }
    public Object getCondition(String conditionName) {
        return conditionList.get(conditionName);
    }

}

