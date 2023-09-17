package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes;

import java.util.Collection;
import java.util.HashMap;

public class StateAccess {
    private final String name;
    private final MetaTypes.AccessType accessType;
    private final HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
    private final HashMap<String, Object> conditionMap; //Store all conditions required during txn-UDF
    public Object udfResult;
    public StateAccess(StateAccessDescription description) {
        name = description.getName();
        stateObjectMap = new HashMap<>();
        accessType = description.getAccessType();
        conditionMap = new HashMap<>();
    }

    public String getName() {
        return name;
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
        conditionMap.put(conditionName, condition);
    }
    public Object getCondition(String conditionName) {
        return conditionMap.get(conditionName);
    }

    public HashMap<String, Object> getConditionMap() {
        return conditionMap;
    }
}

