package intellistream.morphstream.api.operator;

import intellistream.morphstream.api.launcher.MorphStreamEvn;
import intellistream.morphstream.common.io.Rdma.RdmaCompletionListener;
import intellistream.morphstream.common.io.Rdma.RdmaNode;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleConf;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalSpout;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class ApplicationSpoutCombo extends TransactionalSpout {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpoutCombo.class);
    private HashMap<String, TxnDescription> TxnDescriptionHashMap;
    private RdmaNode rdmaNode;
    private Configuration conf = MorphStreamEvn.get().configuration();
    protected ApplicationBolt transactionalBolt;
    public ApplicationSpoutCombo(HashMap<String, TxnDescription> txnDescriptionHashMap) throws Exception {
        super(LOG, 0);
        this.TxnDescriptionHashMap = txnDescriptionHashMap;
        this.transactionalBolt = new ApplicationBolt(txnDescriptionHashMap);
        if (conf.getBoolean("isRemote", false)) {
            this.rdmaNode = new RdmaNode(conf.getString("hostName"), conf.getBoolean("isExecutor"), new RdmaShuffleConf(conf), new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buffer) {

                }

                @Override
                public void onFailure(Throwable exception) {

                }
            });
        }
    }

    @Override
    public void nextTuple() throws InterruptedException {

    }
}
