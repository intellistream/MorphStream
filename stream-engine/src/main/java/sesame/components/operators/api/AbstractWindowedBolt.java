package sesame.components.operators.api;

import org.slf4j.Logger;
import sesame.components.windowing.TupleWindow;
import sesame.execution.runtime.tuple.impl.Marker;

import java.util.Map;

public abstract class AbstractWindowedBolt extends Operator {
    private static final long serialVersionUID = -9211354361283989202L;

    AbstractWindowedBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double branch_selectivity, double read_selectivity, boolean byP, double event_frequency, double window_size) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, byP, event_frequency, window_size);
    }

    AbstractWindowedBolt(double event_frequency, double w) {
        super(null, false, event_frequency, w);
    }


    public abstract void execute(TupleWindow in);


    @Override
    public void callback(int callee, Marker marker) {

    }

}
