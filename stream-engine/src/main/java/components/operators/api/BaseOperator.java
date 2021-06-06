package components.operators.api;

import org.slf4j.Logger;

import java.util.Map;

/**
 * we don't have to differentiate parent operators or parent executors.. just use same list to represent them.
 */
public abstract class BaseOperator extends AbstractBolt {
    private static final long serialVersionUID = 9120945392080191838L;

    protected BaseOperator(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                           boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, byP, event_frequency, w);
    }

    protected BaseOperator(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                           double branch_selectivity, double read_selectivity, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, event_frequency, w);
    }
}
