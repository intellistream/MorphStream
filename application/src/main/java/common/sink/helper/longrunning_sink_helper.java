package common.sink.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static common.CONTROL.enable_log;

/**
 * Created by I309939 on 7/23/2016.
 */
public class longrunning_sink_helper extends helper {
    private static final Logger LOG = LoggerFactory.getLogger(longrunning_sink_helper.class);
    long warm_start = 0, warm_end = 0;

    public longrunning_sink_helper(Logger LOG, int runtime, String metric_path, int thisTaskId) {
        super(runtime, 0, 0, metric_path, thisTaskId, false);
        warm_start = System.nanoTime();
        need_warm_up = false;
    }

    public longrunning_sink_helper(longrunning_sink_helper sink_state, int thisTaskId) {
        super(sink_state.runtimeInSEC, 0, 0, sink_state.metric_path, thisTaskId, false);
        atomic_index_e = sink_state.atomic_index_e;
        start = sink_state.start;
        //end = sink_state.end;
        checkPoint = sink_state.checkPoint;
        need_warm_up = sink_state.need_warm_up;
        print_pid = sink_state.print_pid;
        warm_start = sink_state.warm_start;
        warm_end = sink_state.warm_end;
    }

    public double execute(long bid) {
        atomic_index_e.incrementAndGet();
        if (need_warm_up) {
            warm_end = System.nanoTime();
            if ((warm_end - warm_start) > warm_up)//test if warm up ended.
            {
                need_warm_up = false;
            }
        } else {
            if (print_pid) {//actual processing started.
                sink_pid();
                print_pid = false;
                start = System.nanoTime();
                atomic_index_e.set(0);//clean atomic_index_e
            }
            if (!print_pid) {//actual processing started and not finished..
                end = System.nanoTime();
                if ((end - start) > measure_interval) {
                    checkPoint++;
                    strings.add(String.valueOf((atomic_index_e.get() * 1000000.0 / (end - start))));
                    atomic_index_e.set(0);//clean atomic_index_e
                    if (checkPoint == measure_times) {
                        if(enable_log) LOG.info("finished measurement!");
                        output(strings);
                        return 1;
                    }
                    start = System.nanoTime();
                }
            }
        }
        return 0;
    }
}
