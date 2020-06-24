package common.sink.helper;
import common.helper.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by I309939 on 7/23/2016.
 */
public class stable_sink_helper_bak extends helper {
    private static final Logger LOG = LoggerFactory.getLogger(stable_sink_helper_bak.class);
    long warm_start = 0, warm_end = 0;
    public stable_sink_helper_bak(Logger LOG, int runtime, String metric_path, int thisTaskId) {
        super(runtime, 0, 0, metric_path, thisTaskId, false);
        warm_start = System.nanoTime();
    }
    public boolean execute(String time, boolean print) {
//        local_index_e++;
        if (need_warm_up) {
            warm_end = System.nanoTime();
            if ((warm_end - warm_start) > warm_up)//test if warm up ended.
            {
                need_warm_up = false;
            }
        } else {
            StartMeasurement();
            if (!print_pid) {//actual processing started and not finished..
                if (print) {
                    end = System.nanoTime();
                    //final long event_latency = (end - time) / 1000000;
                    checkPoint++;
                    // final String throughput = String.valueOf((local_index_e * 1000000.0 / (end - start)));
                    final long compute_delay = end - start;
//                    if (print) {
                    final String detailed =
                            "process size" + Event.split_expression
//                                    + local_index_e + Event.split_expression
                                    + "compute delay" + Event.split_expression
                                    + compute_delay + Event.split_expression
                                    + time + Event.split_expression + end;
//                        LOG.info("checkpoint" + Event.split_expression + String.valueOf(checkPoint) + "throughput" + Event.split_expression + throughput
//                                + Event.split_expression + time);
                    strings.add(detailed);
//                    } else {
//                        strings.add(throughput);
//                    }
                    if (checkPoint == measure_times) {
//                        if (!print)
//                            LOG.info("finished measurement!" + calculateAverage(strings));
//                        else
                        LOG.info("finished measurement!");
                        output(strings);
                        return true;
                    }
//					local_index_e = 0; //clean counter
                    start = System.nanoTime();
                }
            }
        }
        return false;
    }
    @Override
    public double execute(long bid) {
        return 0;
    }
}
