package sesame.components.formatter;
import common.collections.Configuration;
import sesame.components.context.TopologyContext;
import sesame.execution.runtime.tuple.impl.Tuple;
public abstract class Formatter {
    TopologyContext context;
    public void initialize(Configuration config, TopologyContext context) {
        this.context = context;
    }
    public abstract String format(Tuple tuple);
}
