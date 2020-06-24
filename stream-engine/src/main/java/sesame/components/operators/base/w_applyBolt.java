package sesame.components.operators.base;
import org.slf4j.Logger;
import sesame.components.operators.api.BaseOperator;
import sesame.components.operators.api.Operator;

import java.util.Map;
/**
 * Created by I309939 on 8/28/2016.
 */
abstract class w_applyBolt extends BaseOperator {
    private static final long serialVersionUID = 742012390912849219L;
    public w_applyBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, byP, event_frequency, w);
    }
    public w_applyBolt(Logger log, Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, null, output_selectivity, byP, event_frequency, w);
    }
    public String output_type() {
        return Operator.w_apply;
    }
}
