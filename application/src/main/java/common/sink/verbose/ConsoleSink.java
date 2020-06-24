package common.sink.verbose;
import common.sink.helper.stable_sink_helper_verbose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.operators.api.BaseSink;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Tuple;

import static common.helper.Event.null_expression;
import static common.helper.Event.split_expression;
public class ConsoleSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);
    private static final long serialVersionUID = -4353933265932894154L;
    int processed = 0;
    private stable_sink_helper_verbose helper;
    private long end;
    private long start;
    private ConsoleSink() {
        super(LOG);
    }
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        helper = new stable_sink_helper_verbose(LOG
                , config.getInt("runtimeInSeconds")
                , config.getString("metrics.output"), getContext().getThisTaskId());
    }
    @Override
    public void execute(Tuple in) throws InterruptedException {
        final String flag = in.getString(3);
        if (flag.equals(null_expression)) {
            if (helper.execute(null, false)) {
                killTopology();
            }
        } else {
            final long receive = System.nanoTime();
            final long event_time = in.getLong(0);
            final String state = "event_time" + split_expression + event_time + split_expression + flag + split_expression + "sink" + split_expression + receive;
            if (helper.execute(state, true)) {
                killTopology();
            }
        }
    }
    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            final String flag = in.getString(3, i);
            if (flag.equals(null_expression)) {
                if (helper.execute(null, false)) {
                    killTopology();
                }
            } else {
                final long receive = System.nanoTime();
                final long event_time = in.getLong(0, i);
                final String state = "event_time" + split_expression + event_time + split_expression + flag + split_expression + "sink" + split_expression + receive;
                if (helper.execute(state, true)) {
                    killTopology();
                }
            }
        }
    }
    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
