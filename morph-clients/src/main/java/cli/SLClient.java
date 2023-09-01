package cli;

import intellistream.morphstream.api.operator.ApplicationSink;
import intellistream.morphstream.api.operator.ApplicationSpoutCombo;
import intellistream.morphstream.api.operator.ApplicationBolt;
import intellistream.morphstream.engine.stream.components.grouping.ShuffleGrouping;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


public class SLClient {
    private static final Logger log = LoggerFactory.getLogger(SLClient.class);

    public static void main(String[] args) throws Exception {
        CliFrontend SLClient = CliFrontend.getOrCreate().appName("SLClient");
        SLClient.LoadConfiguration("/home/resources/SLClient.properties", args);
        SLClient.prepare();

        //TODO:Function fun = new function implements Function(){}
        //TODO:TxnDescription transfer = new TxnDescriptor(new source_table, source_key, condition_table, condition_key, condition, function, type)
        //TODO:TxnDescription deposit = new TxnDescriptor(new source_table, source_key, condition_table, condition_key, condition, function, type)

        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>();
        TxnDescription transferDescription = new TxnDescription();
        txnDescriptions.put("transfer", transferDescription);
        TxnDescription depositDescription = new TxnDescription();
        txnDescriptions.put("deposit", depositDescription);

        ApplicationSpoutCombo spoutCombo = new ApplicationSpoutCombo(txnDescriptions);
        SLClient.evn().topology().builder.setSpout("spout", spoutCombo, 1);
        if (SLClient.evn().configuration().getBoolean("isCombo", true)) {

        } else {
            ApplicationBolt bolt = new ApplicationBolt(txnDescriptions);
            ApplicationSink sink = new ApplicationSink(log);
            SLClient.evn().topology().builder.setBolt("executor", bolt, 1, new ShuffleGrouping("spout"));
            SLClient.evn().topology().builder.setSink("sink", sink, 1, new ShuffleGrouping("executor"));
        }

        SLClient.run();
    }

}
