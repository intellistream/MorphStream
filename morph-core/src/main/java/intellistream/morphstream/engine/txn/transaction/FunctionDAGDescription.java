package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.api.state.FunctionDescription;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;

//TODO: For now, assume each event carries one transaction (txn_length==1)
public class FunctionDAGDescription implements Serializable {
    private final String name;
    private final HashMap<String, FunctionDescription> FunctionDescriptionMap; //Distinguish diff stateAccesses in OPScheduler by their names
    @Getter
    private final HashMap<String, List<String>> transactionCombo = new HashMap<>();

    public FunctionDAGDescription(String name) {
        this.name = name;
        FunctionDescriptionMap = new HashMap<>();
    }

    public void addFunctionDescription(String FunctionName, FunctionDescription description) {
        FunctionDescriptionMap.put(FunctionName, description);
    }

    public FunctionDescription getFunctionDescription(String functionName) {
        return FunctionDescriptionMap.get(functionName);
    }

    public Collection<Map.Entry<String, FunctionDescription>> getFunctionDescEntries() {
        return FunctionDescriptionMap.entrySet();
    }
    public void comboFunctionsIntoTransaction(List<String> functionNames) {
        for (String stateAccessName : functionNames) {
            if (!FunctionDescriptionMap.containsKey(stateAccessName)) {
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
