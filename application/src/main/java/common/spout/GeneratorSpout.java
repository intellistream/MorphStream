package common.spout;

import components.operators.api.AbstractSpout;
import execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static common.Constants.DEFAULT_STREAM_ID;

public class GeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSpout.class);
    private static final long serialVersionUID = 7738169734935576086L;
    private PhoneCallGenerator callGenerator;

    public GeneratorSpout() {
        super(LOG);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int numContestants = 100;
        callGenerator = new PhoneCallGenerator(this.getContext().getThisTaskId(), numContestants);
    }

    @Override
    public void nextTuple() throws InterruptedException {
        collector.emit_bid(DEFAULT_STREAM_ID, callGenerator.receive());
    }
}
