package common.sink;
import common.sink.helper.stable_sink_helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import components.operators.api.BaseSink;
import execution.ExecutionGraph;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Tuple;
/**
 * @author mayconbordin
 */
public class NullSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(NullSink.class);
    private static final long serialVersionUID = 6394306020203560180L;
    private stable_sink_helper helper;
    private NullSink() {
        super(LOG);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , config.getString("metrics.output"), config.getDouble("predict"), 0, thread_Id, false);
    }
    @Override
    public void execute(Tuple input) {
        if (helper.execute(input.getBID()) != 0) {
            killTopology();
        }
    }
    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            if (helper.execute(in.getBID()) != 0) {
                killTopology();
            }
        }
    }
    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
