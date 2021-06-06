package common.sink;

import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PKSink extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(PKSink.class);
    private static final long serialVersionUID = 5481794109405775823L;
    double success = 0;
    double failure = 0;

    @Override
    public void execute(Tuple input) {
        boolean result = input.getBoolean(0);
        if (result) {
            success++;
        } else
            failure++;
        double results;
        results = helper.execute(input.getBID());
        if (results != 0) {
            this.setResults(results);
            LOG.info("Sink finished:" + results);
            if (thisTaskId == graph.getSink().getExecutorID()) {
                measure_end(results);
            }
        }
    }

    public void display() {
        LOG.info("Spikes: " + success + "(" + (success / (success + failure)) + ")");
    }
}
