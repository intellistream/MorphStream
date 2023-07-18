package common.sink;

import common.bolts.transactional.ob.BidingResult;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class OBSink extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(OBSink.class);
    private static final long serialVersionUID = 5481794109405775823L;
    double success = 0;
    double failure = 0;
    int cnt = 0;

    @Override
    public void execute(Tuple input) {
        if (input.getValue(0) instanceof BidingResult) {
            BidingResult result = (BidingResult) input.getValue(0);
            if (result.isSuccess()) {
                success++;
            } else
                failure++;
        }
        check(cnt, input);
        cnt++;
    }

    public void display() {
        if (enable_log) LOG.info("Success: " + success + "(" + (success / (success + failure)) + ")");
        if (enable_log) LOG.info("Failure: " + failure + "(" + (failure / (success + failure)) + ")");
    }
}
