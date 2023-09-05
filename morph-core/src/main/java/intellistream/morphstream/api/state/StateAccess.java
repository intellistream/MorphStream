package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;

import java.lang.reflect.Method;
import java.util.HashMap;

public class StateAccess {
    private final AccessType accessType;
    private final HashMap<String, StateObject> stateObjectMap; //Store all state objects required during txn-UDF
    private String txnUDFName; //Method name of txn UDF, used to invoke txn UDF using Method Reflection during OPScheduler

    public StateAccess(AccessType type) {
        accessType = type;
        stateObjectMap = new HashMap<>();
    }

    public void addStateObject(String stateObjName, AccessType type, String tableName, String keyName) {
        stateObjectMap.put(stateObjName, new StateObject(tableName, keyName, type));
    }

    public StateObject getStateObject(String stateObjName) {
        return stateObjectMap.get(stateObjName);
    }

    public void setTxnUDFName(String udfName) {
        txnUDFName = udfName;
    }

    public String getTxnUDFName() {
        return txnUDFName;
    }
}
