package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.api.state.StateAccessDescription;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;

//TODO: For now, assume each event carries one transaction (txn_length==1)
public class FunctionDescription implements Serializable {
    private final String name;
    private final HashMap<String, StateAccessDescription> stateAccessDescriptionMap; //Distinguish diff stateAccesses in OPScheduler by their names
    @Getter
    private final HashMap<String, List<String>> transactionCombo = new HashMap<>();

    public FunctionDescription(String name) {
        this.name = name;
        stateAccessDescriptionMap = new HashMap<>();
    }

    public void addStateAccess(String stateAccessName, StateAccessDescription description) {
        stateAccessDescriptionMap.put(stateAccessName, description);
    }

    public StateAccessDescription getStateAccess(String stateAccessName) {
        return stateAccessDescriptionMap.get(stateAccessName);
    }

    public Collection<Map.Entry<String, StateAccessDescription>> getStateAccessDescEntries() {
        return stateAccessDescriptionMap.entrySet();
    }
    public void comboFunctionsIntoTransaction(List<String> functionNames) {
        for (String stateAccessName : functionNames) {
            if (!stateAccessDescriptionMap.containsKey(stateAccessName)) {
                throw new RuntimeException("Function " + stateAccessName + " not found in function description");
            }
        }
        for (String key : functionNames) {
            List<String> values = new ArrayList<>(functionNames);
            values.remove(key);
            this.transactionCombo.put(key, values);
        }
    }
    public void display() {
        System.out.println("Function Description: " + name);
    }
}
