package intellistream.morphstream.api.operator.spout;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleConf;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpout;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;


public class ApplicationSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpout.class);
    private RdmaShuffleManager rdmaShuffleManager;
    private Configuration conf = MorphStreamEnv.get().configuration();
    public ApplicationSpout(String id) throws Exception {
        super(id, LOG, 0);
        if (conf.getBoolean("isRemote", false)) {
            this.rdmaShuffleManager = new RdmaShuffleManager(new RdmaShuffleConf(conf), conf.getBoolean("isDriver"));
        }
    }

    @Override
    public void nextTuple() throws InterruptedException {

    }
}
