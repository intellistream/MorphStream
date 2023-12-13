package intellistream.morphstream.engine.stream.components.operators.api.sink;

import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractBolt;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.zeromq.ZMQ;

import java.util.Map;

public abstract class AbstractSink extends AbstractBolt {
    protected int thread_Id;
    protected int tthread;
    public AbstractSink(String id, Logger log, int fid) {
        super(id, log, fid);
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        this.thread_Id = thread_Id;
    }
}
