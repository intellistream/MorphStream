package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes.AccessType;

import java.util.*;

public class StateAccessDescription {
    private final String name;
    private final AccessType accessType;
    private final List<StateObjectDescription> stateObjDescList;
    private String txnUDFName;//Method name of txn UDF, used to invoke txn UDF using Method Reflection during OPScheduler
    private final List<String> valueNames;//Condition refers to values that are not commonly-shared among events, but used in txn-UDF

    public StateAccessDescription(String name, AccessType type) {
        this.name = name;
        accessType = type;
        stateObjDescList = new ArrayList<>();
        valueNames = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void addStateObjectDescription(String stateObjName, AccessType type, String tableName, String keyName, String valueName, int keyIndex) {
        stateObjDescList.add(new StateObjectDescription(stateObjName, type, tableName, keyName, keyIndex));
    }

    public List<StateObjectDescription> getStateObjDescList() {
        return stateObjDescList;
    }

    public void addValueName(String name) {
        valueNames.add(name);
    }

    public List<String> getValueNames() {
        return valueNames;
    }

    public void setTxnUDFName(String name) {
        txnUDFName = name;
    }

    public String getTxnUDFName() {
        return txnUDFName;
    }

    public AccessType getAccessType() {
        return accessType;
    }
}
