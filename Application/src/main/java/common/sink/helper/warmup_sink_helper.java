package common.sink.helper;
import org.slf4j.Logger;
/**
 * Created by I309939 on 7/23/2016.
 */
public class warmup_sink_helper extends helper {
    private final double average_throughput;
    private long periodic_start;
    private long periodic_end;
    public warmup_sink_helper(Logger LOG, int runtime, String metric_path, double average_throughput, int thisTaskId) {
        super(runtime, 0, 0, metric_path, thisTaskId, false);
        this.average_throughput = average_throughput;
    }
    public warmup_sink_helper(warmup_sink_helper sink_state, int thisTaskId) {
        super(sink_state.runtimeInSEC, 0, 0, sink_state.metric_path, thisTaskId, false);
        this.average_throughput = sink_state.average_throughput;
        atomic_index_e = sink_state.atomic_index_e;
        start = sink_state.start;
        //end = sink_state.end;
        checkPoint = sink_state.checkPoint;
        need_warm_up = sink_state.need_warm_up;
        print_pid = sink_state.print_pid;
        periodic_start = sink_state.periodic_start;
        periodic_end = sink_state.periodic_end;
    }
    public double execute(long bid) {
        if (atomic_index_e.get() == 0) {
            start = System.nanoTime();
            periodic_start = System.nanoTime();
        }
        atomic_index_e.incrementAndGet();
        //actual processing started and not finished..
        end = System.nanoTime();
        if ((end - start) > measure_interval) {
            final double v = atomic_index_e.get() * 1000000.0 / (end - start);
            if (v >= average_throughput) {
                periodic_end = System.nanoTime();
                output(periodic_end - periodic_start);
                return 1;
            }
            atomic_index_e.set(0);//clean atomic_index_e
        }
        start = System.nanoTime();
        return 0;
    }
}
