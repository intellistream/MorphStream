package client;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.api.state.FunctionDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;

import java.util.HashMap;

public class Counter extends Client {

    @Override
    public boolean transactionUDF(Function function) {
        StateObject c = function.getStateObject("count");
        function.udfResult = c.getStringValue("count");
        return true;
    }

    @Override
    public Result postUDF(long bid, String txnFlag, HashMap<String, Function> FunctionMap) {
        return new Result(bid);
    }

    @Override
    public void defineFunction() {
        FunctionDAGDescription counter = new FunctionDAGDescription("counter");
        FunctionDescription count = new FunctionDescription("count", MetaTypes.AccessType.WRITE);
        count.addStateObjectDescription("count", MetaTypes.AccessType.WRITE, "count", "count", 0);
        counter.addFunctionDescription("count", count);

        this.txnDescriptions.put("counter", counter);
    }
}
