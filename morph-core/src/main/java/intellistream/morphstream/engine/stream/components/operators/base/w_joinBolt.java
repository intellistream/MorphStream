package intellistream.morphstream.engine.stream.components.operators.base;

import intellistream.morphstream.engine.stream.components.operators.api.WjoinBolt;
import org.slf4j.Logger;

import java.util.Map;

/**
 * Created by I309939 on 8/28/2016.
 */
abstract class w_joinBolt extends WjoinBolt {
    private static final long serialVersionUID = 5220064148086690758L;
    private final Logger LOG;

    public w_joinBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, byP, event_frequency, w);
        LOG = log;
    }

    public w_joinBolt(Logger log, Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, null, output_selectivity, byP, event_frequency, w);
        LOG = log;
    }
}
