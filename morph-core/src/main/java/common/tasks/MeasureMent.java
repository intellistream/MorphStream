package common.tasks;

import java.util.ArrayDeque;

public class MeasureMent {
    protected final ArrayDeque<Long> latency_map = new ArrayDeque();
    public int batch_number_per_wm;
    boolean start_measure = false;
    long start, end;
    private int local_index_e;

    public void start() {
        if (!start_measure) {//only once.
            start = System.nanoTime();//actual processing started.
            local_index_e = 0;
            start_measure = true;
        }
    }

    public void end(int cnt) {
        end = System.nanoTime();
        long time_elapsed = end - start;
        double results = ((double) (cnt) * 1E6 / time_elapsed);//count/ns * 1E6 --> EVENTS/ms
        measure_end(results);
    }

    protected void measure_end(double results) {
    }
}
