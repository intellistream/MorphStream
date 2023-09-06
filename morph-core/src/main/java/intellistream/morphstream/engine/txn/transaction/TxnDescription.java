package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.utils.TxnDataHolder;

import java.util.HashMap;

//TODO: For now, assume each event carries one transaction (txn_length==1)
public class TxnDescription {
    private String name;
    private final HashMap<String, StateAccessDescription> stateAccessDescriptionMap; //Distinguish diff stateAccesses in OPScheduler by their names
    private String postUDFName; //Method name of post-processing UDF, used to invoke post-UDF using Method Reflection in bolts
    private TxnDataHolder dataHolder; //TODO: Holds all utils data used during txn, data are most probably provided by input txnEvent

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

    public void setDataHolder(TxnDataHolder dataHolder) {
        this.dataHolder = dataHolder;
    }

    public TxnDataHolder getDataHolder() {
        return dataHolder;
    }
    public HashMap<String, StateAccessDescription> getStateAccessDescriptionMap() {
        return stateAccessDescriptionMap;
    }
}
