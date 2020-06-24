package sesame.components.operators.api;

import org.slf4j.Logger;

import java.util.Map;

/**
 * Created by I309939 on 8/28/2016.
 */
public abstract class WjoinBolt extends AbstractBolt {
    private static final long serialVersionUID = 3607973933453683397L;

    protected WjoinBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, byP, event_frequency, w);
    }
}
