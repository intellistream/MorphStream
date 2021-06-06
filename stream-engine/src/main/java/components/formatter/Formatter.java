package components.formatter;

import common.collections.Configuration;
import components.context.TopologyContext;
import execution.runtime.tuple.impl.Tuple;

public abstract class Formatter {
    TopologyContext context;

    public void initialize(Configuration config, TopologyContext context) {
        this.context = context;
    }

    public abstract String format(Tuple tuple);
}
