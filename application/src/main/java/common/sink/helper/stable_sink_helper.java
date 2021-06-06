package common.sink.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by I309939 on 7/23/2016.
 */
public class stable_sink_helper extends helper {
    private static final Logger LOG = LoggerFactory.getLogger(stable_sink_helper.class);
    public long warm_start = 0;
    long warm_end = 0;

    //    private int size;
    public stable_sink_helper(Logger LOG, int runtime, String metric_path, double predict, int size, int thisTaskId, boolean measure) {
        super(runtime, predict, size, metric_path, thisTaskId, measure);
        need_warm_up = true;
    }

    public stable_sink_helper(Logger LOG, int runtime, String metric_path, double predict, int size, int thisTaskId) {
        super(runtime, predict, size, metric_path, thisTaskId, false);
        need_warm_up = true;
    }

    public double execute(long bid) {
//        this.size = size;
        return execute("default", bid);
    }

    public double execute(String sourceComponent, long bid) {
//        if (need_warm_up) {
//            if (start) {
//                warm_start = System.nanoTime();
//                start = false;
//            }
//
//            warm_end = System.nanoTime();
//            if ((warm_end - warm_start) > warm_up)//test if warm up ended.
//            {
//                need_warm_up = false;
//                LOG.info("Sink warm up phase finished");
//            }
//        } else {
        StartMeasurement();
        return Measurement(sourceComponent, bid);
//        }
//        return 0;
    }
}
