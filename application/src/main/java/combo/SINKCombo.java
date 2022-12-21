package combo;

import common.Runner;
import common.sink.MeasureSink;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static common.CONTROL.clusterTableSize;
import static common.CONTROL.tweetWindowSize;

public class SINKCombo extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(SINKCombo.class);
    private static final long serialVersionUID = 5481794109405775823L;
    int cnt = 0;
    boolean start_measure = false;
    int global_cnt;
    int window_cnt = 1000 / tweetWindowSize;
    int the_end = window_cnt * clusterTableSize;

    public void start() {
        if (!start_measure) {//only once.
            helper.StartMeasurement();
            start_measure = true;
        }
    }

    public void end(int global_cnt) {
        double results = helper.EndMeasurement(global_cnt);
        measure_end(results);
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        latency_measure(input);
        cnt++;

        //TODO: Properly define the_end
        int the_end = 1000;
        if (cnt == the_end) {
            LOG.info("Sink finishing...");
            end(global_cnt);
        }

    }

    public void display() {
    }
}
