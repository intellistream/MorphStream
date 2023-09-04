package cli;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.operator.ApplicationSink;
import intellistream.morphstream.api.operator.ApplicationSpoutCombo;
import intellistream.morphstream.api.operator.ApplicationBolt;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;
import intellistream.morphstream.engine.stream.components.grouping.ShuffleGrouping;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


public class SLClient {
    private static final Logger log = LoggerFactory.getLogger(SLClient.class);

    //TODO: How to pass event arguments (event.transferAmount) to UDF?
    // Should client get access to TransactionEvent? This UDF should be passed into OPScheduler for execution, we cannot pass-in the entire event.
    public Object transferFunction(double srcBalance, double condBalance, double transferAmount) {
        Object newSrcBalance = null;
        if (condBalance > 100) {
            newSrcBalance = srcBalance - transferAmount;
        }
        return newSrcBalance;
    }

    public Result transferPost(double srcBalance) {
        Result result = new Result();
        //TODO: UDF for post process
//        result.setKeyMap();
        return result;
    }


    public static void main(String[] args) throws Exception {
        CliFrontend SLClient = CliFrontend.getOrCreate().appName("SLClient");
        SLClient.LoadConfiguration("/home/resources/SLClient.properties", args);
        SLClient.prepare();

        //TODO:Function fun = new function implements Function(){}
        //TODO:TxnDescription transfer = new TxnDescriptor(new source_table, source_key, condition_table, condition_key, condition, function, type)
        //TODO:TxnDescription deposit = new TxnDescriptor(new source_table, source_key, condition_table, condition_key, condition, function, type)

        //Define transaction for Combo to execute
        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>();
        TxnDescription transferDescription = new TxnDescription();
        txnDescriptions.put("transfer", transferDescription);
        TxnDescription depositDescription = new TxnDescription();
        txnDescriptions.put("deposit", depositDescription);

        //TODO: return a abstract Record object that carries read result? (StateObject)
        transferDescription.addStateAccess(AccessType.READ, "accounts", "accountID");
        transferDescription.addStateAccess(AccessType.WRITE, "accounts", "accountID");

        /**
         * User just needs to define txn following txn logic: define state accesses, and intermediate UDF
         * The rest (extract sourceRecord, destRecord, condRecord, etc.) are handled by system.
         *
         * TxnDescription txnDescriptor = new TxnDescription()
         * txnDescriptor.addStateAccess("Read_Modify", srcTableName, srcKeyName)
         * txnDescriptor.addStateAccess("Cond", condTableName, condKeyName)
         *
         * UDF function(Record srcRecord, Record condRecord) {
         *     if (condRecordA.getValue(fieldName) > 100) {
         *         srcRecord.getValue(fieldName).incLong(value1)
         *     } else {
         *         srcRecord.getValue(fieldName).incLong(value2)
         *     }
         * }
         *
         * txnDescriptor.addUDF(function(srcRecord, condRecord)) //using java Reflection
         *
         *
         */


        //Define topology
        ApplicationSpoutCombo spoutCombo = new ApplicationSpoutCombo(txnDescriptions);
        SLClient.evn().topology().builder.setSpout("spout", spoutCombo, 1);

        SLClient.run();
    }

}
