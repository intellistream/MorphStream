package client;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes.AccessType;
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;

import java.util.*;


public class BankingSystemClient extends Client {

    /**
     * Client-defined customized txn-UDF, which will be executed in Schedulers
     * access.udfResult is the value to be written into schemaRecord
     *
     * @param access Stores everything bolt needs, including StateObjects updated by Scheduler
     * @return true if txn-UDF is executed successfully, false if txn-UDF is aborted
     */
    @Override
    public boolean transactionUDF(StateAccess access) {
        String txnName = access.getTxnName();
        if (Objects.equals(txnName, "transfer")) {
            String stateAccessName = access.getStateAccessName();
            if (Objects.equals(stateAccessName, "srcTransfer")) {
                StateObject srcAccountState = access.getStateObject("srcAccountState");
                double srcBalance = srcAccountState.getDoubleValue("balance");
                double transferAmount = Double.parseDouble((String) access.getValue("transferAmount"));
                if (srcBalance > 100 && srcBalance > transferAmount) {
                    access.udfResult = srcBalance - transferAmount;
                    return true;
                } else {
                    return false; //abort txn
                }
            } else if (Objects.equals(stateAccessName, "destTransfer")) {
                StateObject destAccountState = access.getStateObject("destAccountState");
                double destBalance = destAccountState.getDoubleValue("balance");
                double transferAmount = Double.parseDouble((String) access.getValue("transferAmount"));
                if (destBalance > 0) {
                    access.udfResult = destBalance + transferAmount;
                    return true;
                } else {
                    return false; //abort txn
                }
            } else {
                return false; //abort txn
            }
        } else if (Objects.equals(txnName, "deposit")) {
            StateObject srcAccountState = access.getStateObject("srcAccountState");
            double srcBalance = srcAccountState.getDoubleValue("balance");
            double depositAmount = Double.parseDouble((String) access.getValue("depositAmount"));
            access.udfResult = srcBalance + depositAmount;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Result postUDF(String txnName, HashMap<String, StateAccess> stateAccessMap) {
        Result result = new Result();
        for (StateAccess stateAccess : stateAccessMap.values()) {
            if (stateAccess.isAborted()) {
                String[] abortResult = {"aborted"};
                result.setResults(abortResult);
                result.setLast(true);
                return result;
            }
        }
        if (Objects.equals(txnName, "transfer")) {
            StateAccess srcTransfer = stateAccessMap.get("srcTransfer");
            StateAccess destTransfer = stateAccessMap.get("destTransfer");
            Double[] stateAccessResults = new Double[2];

            try {
                stateAccessResults[0] = srcTransfer.getStateObject("srcAccountState").getDoubleValue("balance");
            } catch (NullPointerException e) {
                stateAccessResults[0] = 0.0;
            }

            try {
                stateAccessResults[1] = destTransfer.getStateObject("destAccountState").getDoubleValue("balance");
            } catch (NullPointerException e) {
                stateAccessResults[1] = 0.0;
            }

            result.setResults(stateAccessResults);
        } else if (Objects.equals(txnName, "deposit")) {
            StateAccess deposit = stateAccessMap.get("deposit");
            Double[] stateAccessResults = new Double[1];
            stateAccessResults[0] = deposit.getStateObject("srcAccountState").getDoubleValue("balance");
            result.setResults(stateAccessResults);
        }
        result.setLast(true);
        return result;
    }

    @Override
    public void defineFunction() {
        //Define transfer function
        FunctionDescription transferDescriptor = new FunctionDescription("Transfer");
        //Define transfer's 1st state accesses
        StateAccessDescription srcTransfer = new StateAccessDescription("srcTransfer", AccessType.WRITE);
        srcTransfer.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", 0);
        srcTransfer.addValueName("transferAmount");
        //Define transfer's 2nd state accesses
        StateAccessDescription destTransfer = new StateAccessDescription("destTransfer", AccessType.WRITE);
        destTransfer.addStateObjectDescription("destAccountState", AccessType.WRITE, "accounts", "destAccountID", 0);
        destTransfer.addValueName("transferAmount");
        //Add state accesses to transaction
        transferDescriptor.addStateAccess("srcTransfer", srcTransfer);
        transferDescriptor.addStateAccess("destTransfer", destTransfer);
        destTransfer.addFatherName("srcTransfer");
        transferDescriptor.comboFunctionsIntoTransaction(Arrays.asList("srcTransfer", "destTransfer"));
        this.txnDescriptions.put("transfer", transferDescriptor);

        //Define deposit transaction
        FunctionDescription depositDescriptor = new FunctionDescription("Deposit");
        StateAccessDescription deposit = new StateAccessDescription("deposit", AccessType.WRITE);
        deposit.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", 0);
        deposit.addValueName("depositAmount");
        depositDescriptor.addStateAccess("deposit", deposit);
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
