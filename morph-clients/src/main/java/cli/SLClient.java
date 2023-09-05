package cli;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.operator.ApplicationSpoutCombo;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;
import intellistream.morphstream.api.utils.TxnDataHolder;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


public class SLClient {
    private static final Logger log = LoggerFactory.getLogger(SLClient.class);

    /**
     * Client-defined customized txn-UDF, which will be executed in Schedulers
     * This is a callback method using Reflection mechanism, invoked by referencing its className & methodName
     * Before execution, Scheduler should have access to all SchemaRecords required and add them into txnDescriptor
     *
     * @param stateAccess Stores everything bolt needs (transaction info, post-processing UDF)
     * @param dataHolder Let client specify arguments-in-TxnEvent that are used to construct txn or during txn-UDF
     */
//    public void srcTransferFunction(TxnDescription txnDescriptor, TransactionalEvent event) {
//        StateObject srcAccountState = txnDescriptor.getStateAccess("srcTransfer").getStateObject("srcAccountState");
//        double srcBalance = srcAccountState.getDoubleValue("balance");
//        double transferAmount = (double) event.getValueMap().get("transferAmount");
//        if (srcBalance > 100) {
//            srcAccountState.setDoubleValue("balance", srcBalance - transferAmount);
//        }
//    }

    public void srcTransferFunction(StateAccess stateAccess, TxnDataHolder dataHolder) {
        StateObject srcAccountState = stateAccess.getStateObject("srcAccountState");
        double srcBalance = srcAccountState.getDoubleValue("balance");
        double transferAmount = dataHolder.doubleMap.get("transferAmount");
        if (srcBalance > 100) {
            srcAccountState.setDoubleValue("balance", srcBalance - transferAmount);
        }
    }

    public void destTransferFunction(StateAccess stateAccess, TxnDataHolder dataHolder) {
        StateObject srcAccountState = stateAccess.getStateObject("srcAccountState");
        StateObject destAccountState = stateAccess.getStateObject("srcAccountState");
        double srcBalance = srcAccountState.getDoubleValue("balance");
        double transferAmount = dataHolder.doubleMap.get("transferAmount");
        if (srcBalance > 100) {
            destAccountState.setDoubleValue("balance", srcBalance + transferAmount);
        }
    }

    public Result transferPostFunction(TxnDescription txnDescriptor) {
        Result result = new Result();
        Double[] stateAccessResults = new Double[2];
        stateAccessResults[0] = txnDescriptor.getStateAccess("srcTransfer").getStateObject("srcAccountState").getDoubleValue("balance");
        stateAccessResults[1] = txnDescriptor.getStateAccess("destTransfer").getStateObject("destAccountState").getDoubleValue("balance");
        result.setResults(stateAccessResults);
        return result;
    }


    public static void main(String[] args) throws Exception {
        CliFrontend SLClient = CliFrontend.getOrCreate().appName("SLClient");
        SLClient.LoadConfiguration("/home/resources/SLClient.properties", args);
        SLClient.prepare();

        //Initialize transactions for Combo to execute
        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>();

        TxnDataHolder txnDataHolder = new TxnDataHolder();
        txnDataHolder.doubleMap.put("transferAmount", null); //TODO: Pass TxnEvent data into txnDataHolder

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
        //Define state accesses
        StateAccess srcTransfer = new StateAccess(AccessType.MODIFY);
        srcTransfer.addStateObject("srcAccountState", AccessType.MODIFY, "accounts", "accountID");
        srcTransfer.setTxnUDFName("srcTransferFunction"); //Method invoked by its name during reflection

        StateAccess destTransfer = new StateAccess(AccessType.MODIFY);
        destTransfer.addStateObject("srcAccountState", AccessType.MODIFY, "accounts", "accountID");
        destTransfer.setTxnUDFName("destTransferFunction");

        //Add state accesses to transaction
        transferDescriptor.addStateAccess("srcTransfer", srcTransfer);
        transferDescriptor.addStateAccess("destTransfer", destTransfer);

        //Define bolt post-processing
        transferDescriptor.setPostUDFName("transferPostFunction"); //Method invoked by its name during reflection


        //Deposit transaction
        TxnDescription depositDescriptor = new TxnDescription();
        txnDescriptions.put("deposit", depositDescriptor);


        //Define topology
        ApplicationSpoutCombo spoutCombo = new ApplicationSpoutCombo(txnDescriptions);
        SLClient.evn().topology().builder.setSpout("spout", spoutCombo, 1);

        SLClient.run();
    }

}
