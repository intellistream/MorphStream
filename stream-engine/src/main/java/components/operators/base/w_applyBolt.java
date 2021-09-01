package components.operators.base;

import components.operators.api.BaseOperator;
import components.operators.api.Operator;
import org.slf4j.Logger;

import java.util.Map;

/**
 * Created by I309939 on 8/28/2016.
 */
abstract class w_applyBolt extends BaseOperator {
    private static final long serialVersionUID = 742012390912849219L;

    public w_applyBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, w);
    }

    public w_applyBolt(Logger log, Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, null, output_selectivity, w);
    }

    public String output_type() {
        return Operator.w_apply;
    }
}
