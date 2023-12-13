package intellistream.morphstream.engine.stream.components.operators.api.spout;

import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractTransactionalBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;

import java.util.Map;


public abstract class AbstractSpoutCombo extends AbstractSpout {
    protected AbstractTransactionalBolt bolt;
    protected AbstractSink sink;

    public AbstractSpoutCombo(String id, Logger log, int i) {
        super(id, log, i);
        this.scalable = false;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        bolt.loadDB(conf, context, collector);
    }
}
