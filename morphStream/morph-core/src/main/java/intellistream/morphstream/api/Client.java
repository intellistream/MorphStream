package intellistream.morphstream.api;

import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;

import java.util.HashMap;

public abstract class Client {
    public boolean transactionUDF(StateAccess access) {return true;};
    public Result postUDF(String txnFlag, HashMap<String, StateAccess> stateAccessMap) {return null;}
    public String[] execute_txn_udf(String saID, String[] txnData) {return null;}
    public byte[] execute_txn_udf(String saID, byte[] txnData) {return null;}
}
