package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.ClientSideMetaTypes;

import java.util.HashMap;

public class StateAccess {
    private ClientSideMetaTypes.AccessType accessType;
    private HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
    private String txnUDFName;
    public StateAccess(StateAccessDescription description) {
        stateObjectMap = new HashMap<>();
        accessType = description.getAccessType();
        txnUDFName = description.getTxnUDFName();
    }

    public void setStateObject(String stateObjName, StateObject stateObject) {
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
    public String getTxnUDFName() {
        return txnUDFName;
    }
}

