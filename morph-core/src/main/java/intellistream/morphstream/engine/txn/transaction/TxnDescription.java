package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.api.state.StateAccessDescription;

import java.util.Collection;
import java.util.HashMap;

//TODO: For now, assume each event carries one transaction (txn_length==1)
public class TxnDescription {
    private String name;
    private final HashMap<String, StateAccessDescription> stateAccessDescriptionMap; //Distinguish diff stateAccesses in OPScheduler by their names
    private String postUDFName; //Method name of post-processing UDF, used to invoke post-UDF using Method Reflection in bolts

    public TxnDescription() {
        stateAccessDescriptionMap = new HashMap<>();
    }

    public void addStateAccess(String stateAccessName, StateAccessDescription description) {
        stateAccessDescriptionMap.put(stateAccessName, description);
    }

    public StateAccessDescription getStateAccess(String stateAccessName) {
        return stateAccessDescriptionMap.get(stateAccessName);
    }

    public void setPostUDFName(String postUDFName) {
        this.postUDFName = postUDFName;
    }

    public String getPostUDFName() {
        return postUDFName;
    }

    public Collection<StateAccessDescription> getStateAccessDescValues() {
        return stateAccessDescriptionMap.values();
    }
}
