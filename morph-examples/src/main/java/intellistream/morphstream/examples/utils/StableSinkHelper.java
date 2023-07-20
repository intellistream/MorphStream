package intellistream.morphstream.examples.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class StableSinkHelper extends Sinkhelper {
    private static final Logger LOG = LoggerFactory.getLogger(StableSinkHelper.class);
    public long warm_start = 0;
    long warm_end = 0;

    public StableSinkHelper(Logger LOG, int runtime, String metric_path, double predict, int size, int thisTaskId, boolean measure) {
        super(runtime, predict, size, metric_path, thisTaskId);
        need_warm_up = true;
    }

    public StableSinkHelper(Logger LOG, int runtime, String metric_path, double predict, int size, int thisTaskId) {
        super(runtime, predict, size, metric_path, thisTaskId);
        need_warm_up = true;
    }

    public double execute(long bid) {
        return execute("default", bid);
    }

    public double execute(String sourceComponent, long bid) {
        if (need_warm_up) {
            if (warm_start == 0) {
                warm_start = System.nanoTime();
            }

            warm_end = System.nanoTime();
            if ((warm_end - warm_start) > warm_up)//test if warm up ended.
            {
                need_warm_up = false;
                if (enable_log) LOG.info("Sink warm up phase finished");
            }
        } else {
            StartMeasurement();
            return Measurement(sourceComponent, bid);
        }
        return 0;
    }
}
