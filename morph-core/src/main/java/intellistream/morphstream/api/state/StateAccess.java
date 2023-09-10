package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.ClientSideMetaTypes;

import java.util.HashMap;

public class StateAccess {
    private ClientSideMetaTypes.AccessType accessType;
    private HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
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
    public ClientSideMetaTypes.AccessType getAccessType() {
        return accessType;
    }

}

