package client;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.api.state.FunctionDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes.AccessType;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;

import java.util.*;


public class BankingSystemClient extends Client {

    /**
     * Client-defined customized txn-UDF, which will be executed in Schedulers
     * access.udfResult is the value to be written into schemaRecord
     *
     * @param function Stores everything bolt needs, including StateObjects updated by Scheduler
     * @return true if txn-UDF is executed successfully, false if txn-UDF is aborted
     */
    @Override
    public boolean transactionUDF(Function function) {
        String txnName = function.getDAGName();
        if (Objects.equals(txnName, "transfer")) {
            String stateAccessName = function.getFunctionName();
            if (Objects.equals(stateAccessName, "srcTransfer")) {
                StateObject srcAccountState = function.getStateObject("srcAccountState");
                int srcBalance = srcAccountState.getIntValue("balance");
                int transferAmount = Integer.parseInt((String) function.getValue("transferAmount"));
                function.udfResult = srcBalance - transferAmount;
            } else if (Objects.equals(stateAccessName, "destTransfer")) {
                StateObject destAccountState = function.getStateObject("destAccountState");
                int destBalance = destAccountState.getIntValue("balance");
                int transferAmount = Integer.parseInt((String) function.getValue("transferAmount"));
                function.udfResult = destBalance + transferAmount;
            }
        } else if (Objects.equals(txnName, "deposit")) {
            StateObject srcAccountState = function.getStateObject("srcAccountState");
            int srcBalance = srcAccountState.getIntValue("balance");
            int depositAmount = Integer.parseInt((String) function.getValue("depositAmount"));
            function.udfResult = srcBalance + depositAmount;
        }
        return true;
    }

    @Override
    public Result postUDF(long bid, String txnName, HashMap<String, Function> FunctionMap) {
        Result result = new Result(bid);
        for (Function function : FunctionMap.values()) {
            if (function.isAborted()) {
                String[] abortResult = {"aborted"};
                result.setResults(abortResult);
                result.setLast(true);
                return result;
            }
        }
        if (Objects.equals(txnName, "transfer")) {
            Function srcTransfer = FunctionMap.get("srcTransfer");
            Function destTransfer = FunctionMap.get("destTransfer");
            Integer[] stateAccessResults = new Integer[2];

            try {
                stateAccessResults[0] = srcTransfer.getStateObject("srcAccountState").getIntValue("balance");
            } catch (NullPointerException e) {
                stateAccessResults[0] = 0;
            }

            try {
                stateAccessResults[1] = destTransfer.getStateObject("destAccountState").getIntValue("balance");
            } catch (NullPointerException e) {
                stateAccessResults[1] = 0;
            }

            result.setResults(stateAccessResults);
        } else if (Objects.equals(txnName, "deposit")) {
            Function deposit = FunctionMap.get("deposit");
            Integer[] stateAccessResults = new Integer[1];
            stateAccessResults[0] = deposit.getStateObject("srcAccountState").getIntValue("balance");
            result.setResults(stateAccessResults);
        }
        result.setLast(true);
        return result;
    }

    @Override
    public void defineFunction() {
        //Define transfer function
        FunctionDAGDescription transferDescriptor = new FunctionDAGDescription("Transfer");
        //Define transfer's 1st state accesses
        FunctionDescription srcTransfer = new FunctionDescription("srcTransfer", AccessType.WRITE);
        srcTransfer.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", 0);
        srcTransfer.addValueName("transferAmount");
        //Define transfer's 2nd state accesses
        FunctionDescription destTransfer = new FunctionDescription("destTransfer", AccessType.WRITE);
        destTransfer.addStateObjectDescription("destAccountState", AccessType.WRITE, "accounts", "destAccountID", 0);
        destTransfer.addValueName("transferAmount");
        //Add state accesses to transaction
        transferDescriptor.addFunctionDescription("srcTransfer", srcTransfer);
        transferDescriptor.addFunctionDescription("destTransfer", destTransfer);
        destTransfer.addFatherName("srcTransfer");
        //transferDescriptor.comboFunctionsIntoTransaction(Arrays.asList("srcTransfer", "destTransfer"));
        this.txnDescriptions.put("transfer", transferDescriptor);

        //Define deposit transaction
        FunctionDAGDescription depositDescriptor = new FunctionDAGDescription("Deposit");
        FunctionDescription deposit = new FunctionDescription("deposit", AccessType.WRITE);
        deposit.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", 0);
        deposit.addValueName("depositAmount");
        depositDescriptor.addFunctionDescription("deposit", deposit);
        txnDescriptions.put("deposit", depositDescriptor);
    }

    public static void startClient(String[] args) throws Exception {
//        WebServer.createJobInfoJSON("StreamLedger");
        List<Thread> threads = new ArrayList<>();
        int clientNum = MorphStreamEnv.get().configuration().getInt("clientNum");
        for (int threadNum = 0; threadNum < clientNum; threadNum++) {
            Client t = new BankingSystemClient();
            t.initialize(threadNum, MorphStreamEnv.get().clientLatch());
            threads.add(t);
            t.start();
        }
    }
}
