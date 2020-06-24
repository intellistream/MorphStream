package application.sink.helper;

import application.helper.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by I309939 on 7/23/2016.
 */
public class stable_sink_helper_verbose extends helper {
    // long warm_start = 0, warm_end = 0;
    private static final Logger LOG = LoggerFactory.getLogger(stable_sink_helper_verbose.class);
    long starting = System.nanoTime();

    public stable_sink_helper_verbose(Logger LOG, int runtimeInSEC, String metric_path, int thisTaskId) {
        super(runtimeInSEC, 0, 0, metric_path, thisTaskId, false);
        sink_pid();
    }

    public boolean execute(String time, boolean print) {

        if (print) {
            end = System.nanoTime();
            // if ((end - start) > measure_interval) {
            checkPoint++;
            final long compute_delay = end - start;
            final String detailed =
                    "process size" + Event.split_expression
//                            + local_index_e + Event.split_expression
                            + "compute delay" + Event.split_expression
                            + compute_delay + Event.split_expression
                            + time + Event.split_expression + end;
            strings.add(detailed);

            if (end - starting > runtimeInNANO) {
                LOG.info("finished measurement!");
                output(strings);
                return true;
            }
//			local_index_e = 0; //clean counter
            start = System.nanoTime();
            // }
        }
        return false;
    }

    @Override
    public double execute(long bid) {
        return 0;
    }
}
