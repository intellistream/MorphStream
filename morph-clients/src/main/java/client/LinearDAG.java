package client;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.api.state.FunctionDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes.AccessType;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;

import java.util.HashMap;

public class LinearDAG extends Client {
    @Override
    public boolean transactionUDF(Function function) {
        String functionName = function.getFunctionName();
        switch (functionName) {
            case "function_1": {
                StateObject stateObject_1 = function.getStateObject("stateObject_1");
                int value = stateObject_1.getIntValue("value");
                function.udfResult = value + 1;
                break;
            }
            case "function_2": {
                StateObject stateObject_2 = function.getStateObject("stateObject_2");
                int value = stateObject_2.getIntValue("value");
                function.udfResult = value + 2;
                break;
            }
            case "function_3": {
                StateObject stateObject_3 = function.getStateObject("stateObject_3");
                int value = stateObject_3.getIntValue("value");
                function.udfResult = value + 3;
                break;
            }
        }
        return true;
    }

    @Override
    public Result postUDF(long bid, String txnFlag, HashMap<String, Function> FunctionMap) {
        return new Result(bid);
    }

    @Override
    public void defineFunction() {
        FunctionDAGDescription linearDAG = new FunctionDAGDescription("LinearDAG");
        FunctionDescription function_1 = new FunctionDescription("function_1", AccessType.WRITE);
        function_1.addStateObjectDescription("stateObject_1", AccessType.WRITE, "accounts", "id1", 0);
        FunctionDescription function_2 = new FunctionDescription("function_2", AccessType.WRITE);
        function_2.addStateObjectDescription("stateObject_2", AccessType.WRITE, "accounts", "id2", 0);
        FunctionDescription function_3 = new FunctionDescription("function_3", AccessType.WRITE);
        function_3.addStateObjectDescription("stateObject_3", AccessType.WRITE, "accounts", "id3", 0);
        function_2.addFatherName("function_1");
        function_3.addFatherName("function_2");
        linearDAG.addFunctionDescription("function_1", function_1);
        linearDAG.addFunctionDescription("function_2", function_2);
        linearDAG.addFunctionDescription("function_3", function_3);
        this.txnDescriptions.put("LinearDAG", linearDAG);
    }
}
