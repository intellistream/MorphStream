package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;
import intellistream.morphstream.api.utils.UDF;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StateAccessDescription {
    private final AccessType accessType;
    private final HashMap<String, StateObjectDescription> stateObjectDescriptionMap; //Store all state objects required during txn-UDF
    private UDF txnUDF; //Method name of txn UDF, used to invoke txn UDF using Method Reflection during OPScheduler
    private String txnUDFName;

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

    public void setTxnUDF(UDF udf) {
        txnUDF = udf;
    }

    public void setTxnUDFName(String name) {
        txnUDFName = name;
    }

    public String getTxnUDFClassName() {
        return txnUDF.getClassName();
    }

    public String getTxnUDFMethodName() {
        return txnUDF.getMethodName();
    }

    public AccessType getAccessType() {
        return accessType;
    }
}
