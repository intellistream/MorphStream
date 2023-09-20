package intellistream.morphstream.engine.stream.components.operators.api.sink;

import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractBolt;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;

import java.util.Map;

public abstract class AbstractSink extends AbstractBolt {
    public AbstractSink(Logger log, int fid) {
        super(log, fid);
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        throw new UnsupportedOperationException();
    }
}
