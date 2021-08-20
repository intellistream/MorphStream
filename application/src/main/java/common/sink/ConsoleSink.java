package common.sink;

import common.sink.helper.stable_sink_helper;
import components.operators.api.BaseSink;
import execution.ExecutionGraph;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static common.CONTROL.enable_log;

public class ConsoleSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);
    private static final long serialVersionUID = 3026838156176068865L;
    private stable_sink_helper helper;
    private stable_sink_helper helper2;
    private int processed1 = 0;
    private int processed2 = 0;
    private long end;
    private long start;
    private boolean helper_finished = false;
    private boolean helper2_finished = false;

    private ConsoleSink() {
        super(LOG);
    }

    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , config.getString("metrics.output"), config.getDouble("predict"), 0, thread_Id, false);
        helper2 = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , config.getString("metrics.output"), config.getDouble("predict"), 0, thread_Id, false);
    }

    @Override
    public void execute(Tuple input) {
        //not in use.
    }

    @Override
    public void execute(JumboTuple input) {
        int bound = input.length;
        long bid = input.getBID();
        for (int i = 0; i < bound; i++) {
            switch (input.getSourceComponent()) {
                case "rank":
                    if (helper.execute(input.getSourceComponent(), bid) != 0) {
                        helper_finished = true;
                    }
                    if (helper_finished && helper2_finished) {
                        killTopology();
                    }
                    processed1++;
                    if (processed1 % 10000 == 0) {
                        if(enable_log) LOG.info("RANK:" + input.getMsg(i).toString());
                    }
                    break;
                default:
                    if (helper2.execute(input.getSourceComponent(), bid) != 0) {
                        helper2_finished = true;
                    }
                    if (helper_finished && helper2_finished) {
                        killTopology();
                    }
                    processed2++;
                    if (processed2 % 10000 == 0) {
                        if(enable_log) LOG.info("MEDIAN:" + input.getMsg(i).toString());
                    }
                    break;
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
