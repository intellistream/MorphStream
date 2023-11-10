package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.api.state.StateAccessDescription;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

//TODO: For now, assume each event carries one transaction (txn_length==1)
public class FunctionDescription implements Serializable {
    private String name;
    private final HashMap<String, StateAccessDescription> stateAccessDescriptionMap; //Distinguish diff stateAccesses in OPScheduler by their names

    public FunctionDescription() {
        stateAccessDescriptionMap = new HashMap<>();
    }

    public void addStateAccess(String stateAccessName, StateAccessDescription description) {
        stateAccessDescriptionMap.put(stateAccessName, description);
    }

    public StateAccessDescription getStateAccess(String stateAccessName) {
        return stateAccessDescriptionMap.get(stateAccessName);
    }

    public Collection<StateAccessDescription> getStateAccessDescValues() {
        return stateAccessDescriptionMap.values();
    }

    public Collection<Map.Entry<String, StateAccessDescription>> getStateAccessDescEntries() {
        return stateAccessDescriptionMap.entrySet();
    }
}
