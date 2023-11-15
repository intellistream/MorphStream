package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes.AccessType;

import java.io.Serializable;
import java.util.*;

public class StateAccessDescription implements Serializable {
    private final String name;
    private final AccessType accessType;
    private final List<StateObjectDescription> stateObjDescList;
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

    public void addStateObjectDescription(String stateObjName, AccessType type, String tableName, int keyIndex, int fieldIndex) {
        stateObjDescList.add(new StateObjectDescription(stateObjName, type, tableName, keyIndex, fieldIndex));
    }

    public void addStateObjectDescription(StateObjectDescription description) {
        stateObjDescList.add(description);
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

    public AccessType getAccessType() {
        return accessType;
    }
}
