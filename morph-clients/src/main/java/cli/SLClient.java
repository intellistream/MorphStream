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
        String transactionName = access.getTxnName();
        if (Objects.equals(transactionName, "transfer")) {
            String stateAccessName = access.getStateAccessName();
            if (Objects.equals(stateAccessName, "srcTransfer")) {
                StateObject srcAccountState = access.getStateObject("srcAccountState");
                double srcBalance = srcAccountState.getDoubleValue("balance");
                double transferAmount = (double) access.getValue("transferAmount");
                if (srcBalance > 100 && srcBalance > transferAmount) {
                    access.udfResult = srcBalance - transferAmount;
                    return true;
                } else {
                    return false; //abort txn
                }
            } else if (Objects.equals(stateAccessName, "destTransfer")) {
                StateObject srcAccountState = access.getStateObject("srcAccountState");
                StateObject destAccountState = access.getStateObject("destAccountState");
                double srcBalance = srcAccountState.getDoubleValue("balance");
                double destBalance = destAccountState.getDoubleValue("balance");
                double transferAmount = (double) access.getValue("transferAmount");
                if (srcBalance > 100 && srcBalance > transferAmount) {
                    access.udfResult = destBalance + transferAmount;
                    return true;
                } else {
                    return false; //abort txn
                }
            } else {
                return false; //abort txn
            }
        } else if (Objects.equals(transactionName, "deposit")) {
            return false;
        } else {
            return false;
        }
    }

    @Override
    public Result postUDF(HashMap<String, StateAccess> stateAccessMap) {
        Result result = new Result();
        StateAccess srcTransfer = stateAccessMap.get("srcTransfer");
        StateAccess destTransfer = stateAccessMap.get("destTransfer");
        Double[] stateAccessResults = new Double[2];
        stateAccessResults[0] = destTransfer.getStateObject("srcAccountState").getDoubleValue("balance");
        stateAccessResults[1] = destTransfer.getStateObject("destAccountState").getDoubleValue("balance");
        result.setResults(stateAccessResults);
        return result;
    }

    public static void main(String[] args) throws Exception {
        CliFrontend SLClient = CliFrontend.getOrCreate().appName("SLClient");
//        SLClient.LoadConfiguration("/home/resources/SLClient.properties", args);
        SLClient.LoadConfiguration(null, args); //TODO: add loadConfig from file
        SLClient.prepare();

        //Initialize transactions for Combo to execute
        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>(); //Flag -> TxnDescription

        //Transfer transaction
        TxnDescription transferDescriptor = new TxnDescription();
        txnDescriptions.put("transfer", transferDescriptor);
        /**
         * One state access can be defined as one of the follows:
         * READ
         * WRITE
         * WINDOW_READ
         * WINDOW_WRITE
         * NON_DETER_READ
         * NON_DETER_WRITE
         */
        //Define 1st state accesses
        StateAccessDescription srcTransfer = new StateAccessDescription("srcTransfer", AccessType.WRITE);
        srcTransfer.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", "accountValue", 0);
        srcTransfer.addValueName("transferAmount");
        srcTransfer.setTxnUDFName("transactionUDF"); //Method invoked by its name during reflection

        //Define 2nd state accesses
        StateAccessDescription destTransfer = new StateAccessDescription("destTransfer", AccessType.WRITE);
        destTransfer.addStateObjectDescription("srcAccountState", AccessType.READ, "accounts", "srcAccountID", "accountValue", 0);
        destTransfer.addStateObjectDescription("destAccountState", AccessType.WRITE, "accounts", "destAccountID", "accountValue", 1);
        destTransfer.addValueName("transferAmount");
        destTransfer.setTxnUDFName("transactionUDF");

        //Add state accesses to transaction
        transferDescriptor.addStateAccess("srcTransfer", srcTransfer);
        transferDescriptor.addStateAccess("destTransfer", destTransfer);

        //Define bolt post-processing UDF
        transferDescriptor.setPostUDFName("postUDF"); //Method invoked by its name during reflection


        //Deposit transaction
        TxnDescription depositDescriptor = new TxnDescription();
        txnDescriptions.put("deposit", depositDescriptor);
        //...

        //Define topology
        SLClient.setSpoutCombo("spout", txnDescriptions, 1);

//
//        builder.setGlobalScheduler(new SequentialScheduler());
//        Topology topology = builder.createTopology(db, this);

        //Initiate runner
        try {
            SLClient.run();
        } catch (InterruptedException ex) {
            if (enable_log) log.error("Error in running topology locally", ex);
        }
    }

}
