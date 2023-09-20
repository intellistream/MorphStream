package cli;

import intellistream.morphstream.api.operator.spout.ApplicationSpoutCombo;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes.AccessType;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static intellistream.morphstream.configuration.CONTROL.enable_log;


public class SLClient {
    private static final Logger log = LoggerFactory.getLogger(SLClient.class);

    /**
     * Client-defined customized txn-UDF, which will be executed in Schedulers
     * This is a callback method using Reflection mechanism, invoked by referencing its className & methodName
     * Before execution, Scheduler should have access to all SchemaRecords required and add them into StateAccess
     *
     * @param access Stores everything bolt needs, including StateObjects updated by Scheduler
     */

    //Before executing udf, read schemaRecord from tableRecord and write into stateAccess
    //TODO: abstracted common interfaces shared by UDFs (at least an remind client to define txnUDF and postUDF)
    public boolean srcTransferFunction(StateAccess access) {
        StateObject srcAccountState = access.getStateObject("srcAccountState");
        double srcBalance = srcAccountState.getDoubleValue("balance");
        double transferAmount = (double) access.getCondition("transferAmount");
        if (srcBalance > 100 && srcBalance > transferAmount) {
            access.udfResult = srcAccountState.getDoubleValue("balance") - transferAmount;
            return true;
        } else {
            return false; //abort txn
        }
    }
    //after udf, use the udfResult value to update schemaRecord
    //after update to schemaRecord, write the updated schemaRecord to stateAccess

    public boolean destTransferFunction(StateAccess access) {
        StateObject srcAccountState = access.getStateObject("srcAccountState");
        StateObject destAccountState = access.getStateObject("destAccountState");
        double srcBalance = srcAccountState.getDoubleValue("balance");
        double destBalance = destAccountState.getDoubleValue("balance");
        double transferAmount = (double) access.getCondition("transferAmount");
        if (srcBalance > 100 && srcBalance > transferAmount) {
            access.udfResult = srcAccountState.getDoubleValue("balance") + transferAmount;
            return true;
        } else {
            return false;
        }
    }

    //Input: stateAccessMap, which stores all updated stateAccess results under each txn
    public Result transferPostFunction(HashMap<String, StateAccess> stateAccessMap) {
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
        SLClient.LoadConfiguration("/home/resources/SLClient.properties", args);
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
        StateAccessDescription srcTransfer = new StateAccessDescription(AccessType.WRITE);
        srcTransfer.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", "accountValue", 0);
        srcTransfer.addConditionName("transferAmount");
        srcTransfer.setTxnUDFName("srcTransferFunction"); //Method invoked by its name during reflection

        //Define 2nd state accesses
        StateAccessDescription destTransfer = new StateAccessDescription(AccessType.WRITE);
        destTransfer.addStateObjectDescription("srcAccountState", AccessType.READ, "accounts", "srcAccountID", "accountValue", 0);
        destTransfer.addStateObjectDescription("destAccountState", AccessType.WRITE, "accounts", "destAccountID", "accountValue", 1);
        destTransfer.addConditionName("transferAmount");
        destTransfer.setTxnUDFName("destTransferFunction");

        //Add state accesses to transaction
        transferDescriptor.addStateAccess("srcTransfer", srcTransfer);
        transferDescriptor.addStateAccess("destTransfer", destTransfer);

        //Define bolt post-processing UDF
        transferDescriptor.setPostUDFName("transferPostFunction"); //Method invoked by its name during reflection


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
