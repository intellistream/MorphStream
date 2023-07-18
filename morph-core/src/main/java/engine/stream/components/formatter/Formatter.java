package engine.stream.components.formatter;

import common.collections.Configuration;
import engine.stream.components.context.TopologyContext;
import engine.stream.execution.runtime.tuple.impl.Tuple;

public abstract class Formatter {
    TopologyContext context;

    public void initialize(Configuration config, TopologyContext context) {
        this.context = context;
    }

    public abstract String format(Tuple tuple);
}
