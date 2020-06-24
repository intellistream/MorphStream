package common.sink.helper;
import org.slf4j.Logger;
/**
 * Created by I309939 on 7/23/2016.
 */
public class sink_helper extends helper {
    long warm_start = 0, warm_end = 0;
    //    private int size;
    public sink_helper(Logger LOG, int runtime, String metric_path, double predict, int size, int thisTaskId) {
        super(runtime, predict, size, metric_path, thisTaskId, false);
        super.size = size;
        warm_start = System.nanoTime();
        need_warm_up = false;
    }
    public double execute(long bid) {
//        this.size = size;
        return execute("default", bid);
    }
    public double execute(String sourceComponent, long bid) {
        StartMeasurement();
        return Measurement(sourceComponent, bid);
    }
}
