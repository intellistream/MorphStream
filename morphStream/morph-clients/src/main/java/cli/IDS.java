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

public class IDS extends Client {
    private static final Logger log = LoggerFactory.getLogger(IDS.class);

    /**
     * Client-defined customized txn-UDF, which will be executed in Schedulers
     * access.udfResult is the value to be written into schemaRecord
     *
     * @param access Stores everything bolt needs, including StateObjects updated by Scheduler
     * @return true if txn-UDF is executed successfully, false if txn-UDF is aborted
     */
    @Override
    public boolean transactionUDF(StateAccess access) {
        // No need to judge transaction name.
        String packet = (String) access.getValue("packet");
        String[] parts = packet.split(",");
        // By type.
        switch parts[0]:
        case "SSH":
            // Port.
            if parts[1] == "2222"{

            } else{
                access.getStateObject()
            }

        String txnName = access.getTxnName();
        if (Objects.equals(txnName, "transfer")) {
            String stateAccessName = access.getStateAccessName();
            if (Objects.equals(stateAccessName, "srcTransfer")) {
                StateObject srchoststate = access.getStateObject("srchoststate");
                double srcBalance = srchoststate.getDoubleValue("balance");
                double transferAmount = Double.parseDouble((String) access.getValue("transferAmount"));
                if (srcBalance > 100 && srcBalance > transferAmount) {
                    access.udfResult = srcBalance - transferAmount;
                    return true;
                } else {
                    return false; //abort txn
                }
            } else if (Objects.equals(stateAccessName, "mainSA")) {
                StateObject srchoststate = access.getStateObject("srchoststate");
                StateObject desthoststate = access.getStateObject("desthoststate");
                double srcBalance = srchoststate.getDoubleValue("balance");
                double destBalance = desthoststate.getDoubleValue("balance");
                double transferAmount = Double.parseDouble((String) access.getValue("transferAmount"));
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
            StateObject srchoststate = access.getStateObject("srchoststate");
            double srcBalance = srchoststate.getDoubleValue("balance");
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
                return result;
            }
        }
        if (Objects.equals(txnName, "transfer")) {
            StateAccess srcTransfer = stateAccessMap.get("srcTransfer");
            StateAccess mainSA = stateAccessMap.get("mainSA");
            Double[] stateAccessResults = new Double[2];

            try {
                stateAccessResults[0] = srcTransfer.getStateObject("srchoststate").getDoubleValue("balance");
            } catch (NullPointerException e) {
                stateAccessResults[0] = 0.0;
            }

            try {
                stateAccessResults[1] = mainSA.getStateObject("desthoststate").getDoubleValue("balance");
            } catch (NullPointerException e) {
                stateAccessResults[1] = 0.0;
            }

            result.setResults(stateAccessResults);
        } else if (Objects.equals(txnName, "deposit")) {
            StateAccess deposit = stateAccessMap.get("deposit");
            Double[] stateAccessResults = new Double[1];
            stateAccessResults[0] = deposit.getStateObject("srchoststate").getDoubleValue("balance");
            result.setResults(stateAccessResults);
        }
        return result;
    }

    public static void startJob(String[] args) throws Exception {
        CliFrontend slClientJob = new CliFrontend("IDS");
//        SLClient.LoadConfiguration("/home/resources/SLClient.properties", args);
        slClientJob.LoadConfiguration(null, args); //TODO: add loadConfig from file
        slClientJob.prepare();

        //Initialize transactions for Combo to execute
        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>(); //Flag -> TxnDescription

        //Define transfer transaction
        TxnDescription packetHandlerTxn = new TxnDescription();

        StateAccessDescription mainSA = new StateAccessDescription("mainSA", AccessType.WRITE);
        mainSA.addStateObjectDescription("state", AccessType.READ, "hosts", 0, 1);
        mainSA.addStateObjectDescription("state", AccessType.WRITE, "hosts", 1, 1);
        mainSA.addValueName("transferAmount");
        //Add state accesses to transaction
        packetHandlerTxn.addStateAccess("mainSA", mainSA);
        txnDescriptions.put("transfer", packetHandlerTxn);

        //Define deposit transaction
        TxnDescription depositDescriptor = new TxnDescription();
        StateAccessDescription deposit = new StateAccessDescription("deposit", AccessType.WRITE);
        deposit.addStateObjectDescription("srchoststate", AccessType.WRITE, "hosts", 0, 1);
        deposit.addValueName("depositAmount");
        depositDescriptor.addStateAccess("deposit", deposit);
        txnDescriptions.put("deposit", depositDescriptor);

        //Define topology
        slClientJob.setSpoutCombo("ids", txnDescriptions, 4);
        //TODO: let client determine number of DB loader threads, and update in config, then pass to DBInitializer
        //TODO: loadDBThreadNum = total threads of all stateful operators (bolts)

        //Initiate runner
        try {
            slClientJob.start();
        } catch (InterruptedException ex) {
            if (enable_log) log.error("Error in running topology locally", ex);
        }
    }

    public static void main(String[] args) throws Exception {
        WebServer.createJobInfoJSON("StreamLedger");
        startJob(args);
    }
}
