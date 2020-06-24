package common.sink.helper;
import common.helper.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by I309939 on 7/23/2016.
 */
public class stable_sink_helper_multisink extends helper {
    private static final Logger LOG = LoggerFactory.getLogger(stable_sink_helper_multisink.class);
    long warm_start = 0, warm_end = 0;
    int pre_index_e = 0;
    public stable_sink_helper_multisink(Logger LOG, int runtime, String metric_path, int thisTaskId) {
        super(runtime, 0, 0, metric_path, thisTaskId, false);
        sink_pid();
        start = System.nanoTime();
    }
    public boolean execute(String time, boolean print) {
        final int current_index_e = atomic_index_e.incrementAndGet();
        end = System.nanoTime();
        if ((end - start) > measure_interval) {
            //final long event_latency = (end - time) / 1000000;
            checkPoint++;
            final String throughput = String.valueOf(((current_index_e - pre_index_e) * 1000000.0 / (end - start)));
            pre_index_e = current_index_e;
            if (print) {
                final String detailed = "checkpoint" + Event.split_expression + checkPoint + Event.split_expression + "throughput" + Event.split_expression + throughput
                        + Event.split_expression + time + Event.split_expression + end;
                //  LOG.info(detailed);
                strings.add(detailed);
            } else {
                strings.add(throughput);
            }
            if (checkPoint == measure_times) {
                if (!print)
//                    LOG.info("finished measurement!" + calculateAverage(strings));
                    output(strings);
                return true;
            }
            start = System.nanoTime();
        }
        return false;
    }
    @Override
    public double execute(long bid) {
        return 0;
    }
}
