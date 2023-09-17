package intellistream.morphstream.api;

import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;

import java.util.HashMap;

public abstract class Client {
    public abstract boolean transactionUDF(StateAccess access);
    public abstract Result postUDF(HashMap<String, StateAccess> stateAccessMap);
}
