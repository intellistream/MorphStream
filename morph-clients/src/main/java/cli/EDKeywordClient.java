package cli;

import intellistream.morphstream.api.operator.ApplicationSink;
import intellistream.morphstream.api.operator.ApplicationSpout;
import intellistream.morphstream.api.operator.ApplicationSpoutCombo;
import intellistream.morphstream.api.operator.ApplicationBolt;
import intellistream.morphstream.engine.stream.components.grouping.ShuffleGrouping;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * An example client with multiple operators, performs the first stage of ED: burst keyword detection
 * Topology: Spout -> Tweet Registrant -> Word Updater -> Trend Calculator -> Sink
 */
public class EDKeywordClient {
    private static final Logger log = LoggerFactory.getLogger(EDKeywordClient.class);

    public static void main(String[] args) throws Exception {
        CliFrontend EDKeywordClient = CliFrontend.getOrCreate().appName("EDKeywordClient");
        EDKeywordClient.LoadConfiguration("/home/resources/EDKeywordClient.properties", args);
        EDKeywordClient.prepare();

        //TODO:Function fun = new function implements Function(){}
        //TODO:TxnDescription transfer = new TxnDescriptor(new source_table, source_key, condition_table, condition_key, condition, function, type)
        //TODO:TxnDescription deposit = new TxnDescriptor(new source_table, source_key, condition_table, condition_key, condition, function, type)

        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>();
        TxnDescription transferDescription = new TxnDescription();
        txnDescriptions.put("transfer", transferDescription);
        TxnDescription depositDescription = new TxnDescription();
        txnDescriptions.put("deposit", depositDescription);

        ApplicationSpoutCombo spoutCombo = new ApplicationSpoutCombo(txnDescriptions);
        EDKeywordClient.env().transactionalTopology().builder.setSpout("spout", spoutCombo, 1);

        //Define topology
        ApplicationSpout spout = new ApplicationSpout(txnDescriptions); //TODO: For non-combo, what to specify for spout and bolt?
        ApplicationBolt bolt = new ApplicationBolt(txnDescriptions);
        ApplicationSink sink = new ApplicationSink(log);

        EDKeywordClient.env().transactionalTopology().builder.setSpout("executor", spout, 1);
        EDKeywordClient.env().transactionalTopology().builder.setBolt("executor", bolt, 1, new ShuffleGrouping("spout"));
        EDKeywordClient.env().transactionalTopology().builder.setSink("sink", sink, 1, new ShuffleGrouping("executor"));


        EDKeywordClient.run();
    }

}
