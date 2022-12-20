package components.operators.base;

import common.Constants;
import components.operators.api.BaseOperator;
import org.slf4j.Logger;

import java.util.Map;

public abstract class FilterBolt extends BaseOperator {
    private static final long serialVersionUID = 234241824251364743L;

    protected FilterBolt() {
        super(null, null, null, 0.5, 1, 1);
    }

    protected FilterBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity) {
        super(log, input_selectivity, output_selectivity, 1);
    }

    protected FilterBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double read_selectivity) {
        super(log, input_selectivity, output_selectivity, 1, read_selectivity, 1);
    }

    protected FilterBolt(Logger log, Map<String, Double> output_selectivity) {
        super(log, null, output_selectivity, 1);
    }

    public String output_type() {
        return Constants.filter;
    }
}
