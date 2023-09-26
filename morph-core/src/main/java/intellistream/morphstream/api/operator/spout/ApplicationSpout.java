package intellistream.morphstream.api.operator.spout;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleConf;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApplicationSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpout.class);
    private RdmaShuffleManager rdmaShuffleManager;
    private Configuration conf = MorphStreamEnv.get().configuration();
    public ApplicationSpout() throws Exception {
        super(LOG, 0);
        if (conf.getBoolean("isRemote", false)) {
            this.rdmaShuffleManager = new RdmaShuffleManager(new RdmaShuffleConf(conf), conf.getBoolean("isDriver"));
        }
    }

    @Override
    public void nextTuple() throws InterruptedException {
        //TODO: implement nextTuple for non-combo spout
    }
}
