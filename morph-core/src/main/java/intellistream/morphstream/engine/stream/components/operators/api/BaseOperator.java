package intellistream.morphstream.engine.stream.components.operators.api;

import org.slf4j.Logger;

import java.util.Map;

/**
 * we don't have to differentiate parent operators or parent executors.. just use same list to represent them.
 */
public abstract class BaseOperator extends AbstractBolt {
    private static final long serialVersionUID = 9120945392080191838L;

    protected BaseOperator(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                           double w) {
        super(log, input_selectivity, output_selectivity, w);
    }

    protected BaseOperator(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                           double branch_selectivity, double read_selectivity, double w) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, w);
    }
}
