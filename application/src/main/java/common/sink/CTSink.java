package common.sink;

import common.bolts.transactional.sl.TransactionResult;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static common.CONTROL.enable_log;

public class CTSink extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(CTSink.class);
    private static final long serialVersionUID = 5481794109405775823L;
    double success = 0;
    double failure = 0;
    int cnt = 0;

    @Override
    public void execute(Tuple input) {
        if (input.getValue(0) instanceof TransactionResult) {
            TransactionResult result = (TransactionResult) input.getValue(0);
            if (result.isSuccess()) {
                success++;
            } else
                failure++;
        }
        check(cnt, input);
        cnt++;
    }

    public void display() {
        if(enable_log) LOG.info("Success: " + success + "(" + (success / (success + failure)) + ")");
        if(enable_log) LOG.info("Failure: " + failure + "(" + (failure / (success + failure)) + ")");
    }
}
