package engine.stream.components.operators.api;

import engine.stream.components.windowing.TupleWindow;
import org.slf4j.Logger;

import java.util.Map;

public abstract class AbstractWindowedBolt extends Operator {
    private static final long serialVersionUID = -9211354361283989202L;

    AbstractWindowedBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double branch_selectivity, double read_selectivity, boolean byP, double event_frequency, double window_size) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, window_size);
    }

    AbstractWindowedBolt(double event_frequency, double w) {
        super(null, w);
    }

    public abstract void execute(TupleWindow in);

}
