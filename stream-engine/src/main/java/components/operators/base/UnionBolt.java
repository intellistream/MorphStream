package components.operators.base;

import common.Constants;
import components.operators.api.BaseOperator;
import org.slf4j.Logger;

import java.util.Map;

/**
 * Created by I309939 on 8/28/2016.
 */
public abstract class UnionBolt extends BaseOperator {
    private static final long serialVersionUID = 4285498526255572237L;

    // private static final Logger LOG = LoggerFactory.getLogger(unionBolt.class);
    public UnionBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity) {
        super(log, input_selectivity, output_selectivity, 1);
    }

    protected UnionBolt(Logger log) {
        super(log, null,
                null, 1);
    }

    protected UnionBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                        double branch_selectivity, double read_selectivity) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, 0);
    }

    public String output_type() {
        return Constants.reduce;
    }
}
