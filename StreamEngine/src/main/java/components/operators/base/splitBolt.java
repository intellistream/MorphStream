package components.operators.base;
import org.slf4j.Logger;
import components.operators.api.BaseOperator;

import java.util.Map;
/**
 * Created by tony on 5/8/2017.
 */
public abstract class splitBolt extends BaseOperator {
    private static final long serialVersionUID = 5684338552926136996L;
    public splitBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                     double branch_selectivity, double read_selectivity) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, 1, 1);
    }
    public splitBolt(Logger log, Map<String, Double> output_selectivity) {
        super(log, null, output_selectivity, false, 0, 1);
    }
}
