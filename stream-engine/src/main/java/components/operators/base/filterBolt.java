package components.operators.base;

import components.operators.api.BaseOperator;
import components.operators.api.Operator;
import org.slf4j.Logger;

import java.util.Map;

/**
 * Created by I309939 on 8/28/2016.
 */
public abstract class filterBolt extends BaseOperator {
    private static final long serialVersionUID = 234241824251364743L;

    protected filterBolt() {
        super(null, null, null, 0.5, 1, 1);
    }

    protected filterBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity) {
        super(log, input_selectivity, output_selectivity, 1);
    }

    protected filterBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double read_selectivity) {
        super(log, input_selectivity, output_selectivity, 1, read_selectivity, 1);
    }

    protected filterBolt(Logger log, Map<String, Double> output_selectivity) {
        super(log, null, output_selectivity, 1);
    }

    public String output_type() {
        return Operator.filter;
    }
}
