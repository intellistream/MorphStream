package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes.AccessType;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;

public class FunctionDescription implements Serializable {
    @Getter
    private final String name;
    @Getter
    private final AccessType accessType;
    @Getter
    private final List<StateObjectDescription> stateObjDescList;
    @Getter
    private final List<String> fatherNames;
    @Getter
    private final List<String> paraNames;//Condition refers to values that are not commonly-shared among events, but used in txn-UDF

    public FunctionDescription(String name, AccessType type) {
        this.name = name;
        accessType = type;
        stateObjDescList = new ArrayList<>();
        paraNames = new ArrayList<>();
        fatherNames = new ArrayList<>();
    }

    public void addStateObjectDescription(String stateObjName, AccessType type, String tableName, String keyName, int keyIndex) {
        stateObjDescList.add(new StateObjectDescription(stateObjName, type, tableName, keyName, keyIndex));
    }

    public void addParaName(String name) {
        paraNames.add(name);
    }
    public void addFatherName(String name) {
        fatherNames.add(name);
    }
}
