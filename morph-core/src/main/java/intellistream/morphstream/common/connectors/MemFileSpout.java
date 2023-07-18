package intellistream.morphstream.common.connectors;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.AbstractSpout;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class MemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;
    protected int element = 0;

    public MemFileSpout() {
        super(LOG);
        this.scalable = false;
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 2;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        if (enable_log) LOG.info("Spout initialize is being called");
        cnt = 0;
        counter = 0;
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        load_input();
    }

    @Override
    public void nextTuple() throws InterruptedException {
        collector.emit(0, array.get(counter));
        counter++;
        if (counter == array.size()) {
            counter = 0;
        }
    }

    public void display() {
        if (enable_log) LOG.info("timestamp_counter:" + counter);
    }
}