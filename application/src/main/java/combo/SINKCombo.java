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
    int stop_event_count = 0;

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
    public void execute(Tuple input) throws InterruptedException { //receives ESEvent
        latency_measure(input);
//        cnt++;
        if ((!enable_app_combo) && input.getBID() >= totalEvents-1) { //TODO: Only perform this for Non_Combo, refine it.
            stop_event_count++;
            if (stop_event_count == tthread) {
                LOG.info("Sink has received outputs: " + cnt);
                end(totalEvents);
            }
        }
    }

    public void display() {
    }
}
