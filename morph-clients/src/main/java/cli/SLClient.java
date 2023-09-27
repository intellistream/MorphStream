package cli;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes.AccessType;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Objects;

import static intellistream.morphstream.configuration.CONTROL.enable_log;


public class SLClient extends Client {
    private static final Logger log = LoggerFactory.getLogger(SLClient.class);

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
                float srcBalance = srcAccountState.getFloatValue("balance");
                float transferAmount = Float.parseFloat((String) access.getValue("transferAmount"));
                if (srcBalance > 100 && srcBalance > transferAmount) {
                    access.udfResult = srcBalance - transferAmount;
                    return true;
                } else {
                    return false; //abort txn
                }
            } else if (Objects.equals(stateAccessName, "destTransfer")) {
                StateObject srcAccountState = access.getStateObject("srcAccountState");
                StateObject destAccountState = access.getStateObject("destAccountState");
                float srcBalance = srcAccountState.getFloatValue("balance");
                float destBalance = destAccountState.getFloatValue("balance");
                float transferAmount = Float.parseFloat((String) access.getValue("transferAmount"));
                if (srcBalance > 100 && srcBalance > transferAmount) {
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
            float srcBalance = srcAccountState.getFloatValue("balance");
            float depositAmount = Float.parseFloat((String) access.getValue("depositAmount"));
            access.udfResult = srcBalance + depositAmount;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Result postUDF(String txnName, HashMap<String, StateAccess> stateAccessMap) {
        Result result = new Result();
        if (Objects.equals(txnName, "transfer")) {
            StateAccess srcTransfer = stateAccessMap.get("srcTransfer");
            StateAccess destTransfer = stateAccessMap.get("destTransfer");
            Double[] stateAccessResults = new Double[2];
            stateAccessResults[0] = srcTransfer.getStateObject("srcAccountState").getDoubleValue("balance");
            stateAccessResults[1] = destTransfer.getStateObject("destAccountState").getDoubleValue("balance");
            result.setResults(stateAccessResults);
        } else if (Objects.equals(txnName, "deposit")) {
            StateAccess deposit = stateAccessMap.get("deposit");
            Double[] stateAccessResults = new Double[1];
            stateAccessResults[0] = deposit.getStateObject("srcAccountState").getDoubleValue("balance");
            result.setResults(stateAccessResults);
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        CliFrontend SLClient = CliFrontend.getOrCreate().appName("SLClient");
//        SLClient.LoadConfiguration("/home/resources/SLClient.properties", args);
        SLClient.LoadConfiguration(null, args); //TODO: add loadConfig from file
        SLClient.prepare();

        //Initialize transactions for Combo to execute
        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>(); //Flag -> TxnDescription

        //Define transfer transaction
        TxnDescription transferDescriptor = new TxnDescription();
        //Define transfer's 1st state accesses
        StateAccessDescription srcTransfer = new StateAccessDescription("srcTransfer", AccessType.WRITE);
        srcTransfer.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", "accountValue", 0);
        srcTransfer.addValueName("transferAmount");
        //Define transfer's 2nd state accesses
        StateAccessDescription destTransfer = new StateAccessDescription("destTransfer", AccessType.WRITE);
        destTransfer.addStateObjectDescription("srcAccountState", AccessType.READ, "accounts", "srcAccountID", "accountValue", 0);
        destTransfer.addStateObjectDescription("destAccountState", AccessType.WRITE, "accounts", "destAccountID", "accountValue", 1);
        destTransfer.addValueName("transferAmount");
        //Add state accesses to transaction
        transferDescriptor.addStateAccess("srcTransfer", srcTransfer);
        transferDescriptor.addStateAccess("destTransfer", destTransfer);
        txnDescriptions.put("transfer", transferDescriptor);

        //Define deposit transaction
        TxnDescription depositDescriptor = new TxnDescription();
        StateAccessDescription deposit = new StateAccessDescription("deposit", AccessType.WRITE);
        deposit.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", "accountValue", 0);
        deposit.addValueName("depositAmount");
        depositDescriptor.addStateAccess("deposit", deposit);
        txnDescriptions.put("deposit", depositDescriptor);

        //Define topology
        SLClient.setSpoutCombo("spout", txnDescriptions, 4);
        //TODO: let client determine number of DB loader threads, and update in config, then pass to DBInitializer
        //TODO: loadDBThreadNum = total threads of all stateful operators (bolts)

        //Initiate runner
        try {
            SLClient.run();
        } catch (InterruptedException ex) {
            if (enable_log) log.error("Error in running topology locally", ex);
        }
    }

}
