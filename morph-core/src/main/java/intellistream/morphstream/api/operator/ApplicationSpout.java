package intellistream.morphstream.api.operator;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleConf;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalSpout;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class ApplicationSpout extends TransactionalSpout {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpout.class);
    private HashMap<String, TxnDescription> TxnDescriptionHashMap;
    private RdmaShuffleManager rdmaShuffleManager;
    private Configuration conf = MorphStreamEnv.get().configuration();
    protected ApplicationBolt transactionalBolt;
    public ApplicationSpout(HashMap<String, TxnDescription> txnDescriptionHashMap) throws Exception {
        super(LOG, 0);
        this.TxnDescriptionHashMap = txnDescriptionHashMap;
        this.transactionalBolt = new ApplicationBolt(txnDescriptionHashMap);
        if (conf.getBoolean("isRemote", false)) {
            this.rdmaShuffleManager = new RdmaShuffleManager(new RdmaShuffleConf(conf), conf.getBoolean("isDriver"));
        }
    }

    @Override
    public void nextTuple() throws InterruptedException {

    }
}
