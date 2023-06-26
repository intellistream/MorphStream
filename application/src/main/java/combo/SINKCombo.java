package combo;

import common.sink.MeasureSink;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static common.CONTROL.*;

public class SINKCombo extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(SINKCombo.class);
    private static final long serialVersionUID = 5481794109405775823L;
//    int cnt = 0;
    boolean start_measure = false;
    int global_cnt;
    int window_cnt = totalEvents / tweetWindowSize;
    int the_end = window_cnt * clusterTableSize; //each window outputs $clusterTableSize number of events

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
//        cnt++;
//        if ((!enable_app_combo) && cnt >= the_end) { //TODO: Only perform this for ED, refine it.
//            LOG.info("Sink has received outputs: " + cnt);
//            end(global_cnt);
//        }
    }

    public void display() {
    }
}
