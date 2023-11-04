package utils.java.StateAccess;

import java.util.Collection;
import java.util.HashMap;

public class StateAccess {
    private final HashMap<String, String> stateObjectMap; //Store all state objects required during txn-UDF
    private final HashMap<String, Object> conditionList; //Store all conditions required during txn-UDF
    public Object udfResult;
    public StateAccess() {
        stateObjectMap = new HashMap<>();
        conditionList = new HashMap<>();
    }

    public void addStateObject(String stateObjName, String stateObject) {
        stateObjectMap.put(stateObjName, stateObject);
    }
    public String getStateObject(String stateObjName) {
        return stateObjectMap.get(stateObjName);
    }
    public Collection<String> getStateObjects() {
        return stateObjectMap.values();
    }

    public void addCondition(String conditionName, Object condition) {
        conditionList.put(conditionName, condition);
    }
    public Object getCondition(String conditionName) {
        return conditionList.get(conditionName);
    }		
}