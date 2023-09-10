package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StateAccessDescription {
    private final AccessType accessType;
    private final HashMap<String, StateObjectDescription> stateObjectDescriptionMap; //Store all state objects required during txn-UDF
    private String txnUDFName; //Method name of txn UDF, used to invoke txn UDF using Method Reflection during OPScheduler

    public StateAccessDescription(AccessType type) {
        accessType = type;
        stateObjectDescriptionMap = new HashMap<>();
    }

    public void addStateObjectDescription(String stateObjName, AccessType type, String tableName, String keyName, String valueName, int keyIndex) {
        stateObjectDescriptionMap.put(stateObjName, new StateObjectDescription(type, tableName, keyName, valueName, keyIndex));
    }

    public StateObjectDescription getStateObjectDescription(String stateObjName) {
        return stateObjectDescriptionMap.get(stateObjName);
    }
    public HashMap<String, StateObjectDescription> getStateObjectDescriptionMap() {
        return stateObjectDescriptionMap;
    }

    public Set<Map.Entry<String, StateObjectDescription>> getStateObjectEntries() {
        return stateObjectDescriptionMap.entrySet();
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
