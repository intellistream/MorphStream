package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;

import java.util.HashMap;

public class StateAccessDescription {
    private final AccessType accessType;
    private final HashMap<String, StateObjectDescription> stateObjectMap; //Store all state objects required during txn-UDF
    private String txnUDFName; //Method name of txn UDF, used to invoke txn UDF using Method Reflection during OPScheduler

    public StateAccessDescription(AccessType type) {
        accessType = type;
        stateObjectMap = new HashMap<>();
    }

    public void addStateObjectDescription(String stateObjName, AccessType type, String tableName, String keyName, String valueName) {
        stateObjectMap.put(stateObjName, new StateObjectDescription(type, tableName, keyName, valueName));
    }

    public StateObjectDescription getStateObjectDescription(String stateObjName) {
        return stateObjectMap.get(stateObjName);
    }
    public HashMap<String, StateObjectDescription> getStateObjectMap() {
        return stateObjectMap;
    }

    public void setTxnUDFName(String udfName) {
        txnUDFName = udfName;
    }

    public String getTxnUDFName() {
        return txnUDFName;
    }
    public AccessType getAccessType() {
        return accessType;
    }
}
