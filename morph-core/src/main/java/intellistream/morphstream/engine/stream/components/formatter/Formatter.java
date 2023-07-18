package intellistream.morphstream.engine.stream.components.formatter;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;

public abstract class Formatter {
    TopologyContext context;

    public void initialize(Configuration config, TopologyContext context) {
        this.context = context;
    }

    public abstract String format(Tuple tuple);
}
