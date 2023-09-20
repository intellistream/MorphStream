package intellistream.morphstream.api.operator.spout;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.MorphStreamBolt;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleConf;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpoutCombo;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.Constants.*;

public class ApplicationSpoutCombo extends AbstractSpoutCombo {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpoutCombo.class);
    private HashMap<String, TxnDescription> TxnDescriptionHashMap;
    private RdmaShuffleManager rdmaShuffleManager;
    private Configuration conf = MorphStreamEnv.get().configuration();
    public ApplicationSpoutCombo(HashMap<String, TxnDescription> txnDescriptionHashMap) throws Exception {
        super(LOG, 0);
        this.TxnDescriptionHashMap = txnDescriptionHashMap;
        if (conf.getBoolean("isRemote", false)) {
            this.rdmaShuffleManager = new RdmaShuffleManager(new RdmaShuffleConf(conf), conf.getBoolean("isDriver"));
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        switch (config.getInt("CCOption", 0)) {
            case CCOption_MorphStream: {//T-Stream
                bolt = new MorphStreamBolt(TxnDescriptionHashMap, 0);
                break;
            }
            case CCOption_SStore:
                bolt = new SStoreBolt(TxnDescriptionHashMap, 0);
            default:
                if (enable_log) LOG.error("Please select correct CC option!");
        }
    }
}
