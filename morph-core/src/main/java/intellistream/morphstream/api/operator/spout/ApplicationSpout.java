package intellistream.morphstream.api.operator.spout;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApplicationSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpout.class);
    private Configuration conf = MorphStreamEnv.get().configuration();
    public ApplicationSpout(String id) throws Exception {
        super(id, LOG, 0);
    }

    @Override
    public void nextTuple() throws InterruptedException {

    }
}
