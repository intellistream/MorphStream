package cli;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.operator.ApplicationSpoutCombo;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;
import intellistream.morphstream.api.utils.TxnDataHolder;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.controller.input.scheduler.SequentialScheduler;
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
     * Before execution, Scheduler should have access to all SchemaRecords required and add them into txnDescriptor
     *
     * @param access Stores everything bolt needs (transaction info, post-processing UDF)
     * @param dataHolder Let client specify arguments-in-TxnEvent that are used to construct txn or during txn-UDF
     */
    public boolean srcTransferFunction(StateAccess access, TxnDataHolder dataHolder) {
        StateObject srcAccountState = access.getStateObject("srcAccountState");
        double srcBalance = srcAccountState.getDoubleValue("balance");
        double transferAmount = dataHolder.doubleMap.get("transferAmount");
        if (srcBalance > 100 && srcBalance > transferAmount) {
            srcAccountState.setDoubleValue("balance", srcBalance - transferAmount);
            return true;
        } else {
            return false;
        }
    }

    public boolean destTransferFunction(StateAccess access, TxnDataHolder dataHolder) { //TODO: For some app, need to pass-in event data to txnUDF. E.g. In OGScheduler, each TPEvent's own speed
        StateObject srcAccountState = access.getStateObject("srcAccountState");
        StateObject destAccountState = access.getStateObject("destAccountState");
        double srcBalance = srcAccountState.getDoubleValue("balance");
        double destBalance = destAccountState.getDoubleValue("balance");
        double transferAmount = dataHolder.doubleMap.get("transferAmount");
        if (srcBalance > 100 && srcBalance > transferAmount) {
            destAccountState.setDoubleValue("balance", destBalance + transferAmount);
            return true;
        } else {
            return false;
        }
    }

    public Result transferPostFunction(StateAccess access, TransactionalEvent event) {
        Result result = new Result();
        Double[] stateAccessResults = new Double[2];
        stateAccessResults[0] = access.getStateObject("srcAccountState").getDoubleValue("balance");
        stateAccessResults[1] = access.getStateObject("destAccountState").getDoubleValue("balance");
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
         * 1. Read only (read from one or more records)
         * 2. Write only (write to one record)
         * 3. Modify (read from one or more records, then write to one record)
         * Note: for each state access, client needs to define its UDF (avoid if-else during UDF execution)
         */
        //Define 1st state accesses
        StateAccessDescription srcTransfer = new StateAccessDescription(AccessType.WRITE);
        srcTransfer.addStateObjectDescription("srcAccountState", AccessType.WRITE, "accounts", "srcAccountID", "accountValue", 0);
//        UDF srcTransferUDF = new UDF("SLClient", "srcTransferFunction");
        srcTransfer.setTxnUDFName("srcTransferFunction"); //Method invoked by its name during reflection

        //Define 2nd state accesses
        StateAccessDescription destTransfer = new StateAccessDescription(AccessType.WRITE);
        destTransfer.addStateObjectDescription("srcAccountState", AccessType.READ, "accounts", "srcAccountID", "accountValue", 0);
        destTransfer.addStateObjectDescription("destAccountState", AccessType.WRITE, "accounts", "destAccountID", "accountValue", 1);
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
        ApplicationSpoutCombo spoutCombo = new ApplicationSpoutCombo(txnDescriptions);
        SLClient.setSpout("spout", spoutCombo, 1);
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
