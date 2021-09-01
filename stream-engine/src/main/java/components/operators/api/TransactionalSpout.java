package components.operators.api;

import common.tools.FastZipfGenerator;
import execution.runtime.tuple.impl.Marker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;

import static common.CONTROL.enable_log;
import static profiler.Metrics.NUM_ACCESSES;

public abstract class TransactionalSpout extends AbstractSpout implements Checkpointable {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalSpout.class);
    public transient FastZipfGenerator p_generator;
    public long epoch_size = 0;
    public double target_Hz;
    public double checkpoint_interval;
    public volatile int control = 0;//control how many elements in each epoch.
    public int _combo_bid_size = 1;
    public int counter = 0;
    public transient BufferedWriter writer;
    public int taskId;
    public int ccOption;
    public long bid = 0;//local bid.
    public int empty = 0;//execute without emit.
    public int batch_number_per_wm;

    protected TransactionalSpout(Logger log, int fid) {
        super(log);
        this.fid = fid;
    }

    public double getEmpty() {
        return empty;
    }

    @Override
    public abstract void nextTuple() throws InterruptedException;

    /**
     * THIS IS USED ONLY WHEN "enable_app_combo" is true.
     * Everytime a thread emits "batch_size" tuples, it emits a signal to trigger txn processing.
     *
     * @param counter
     */
    @Override
    public boolean checkpoint(int counter) {
        return (counter % checkpoint_interval == 0);
    }

    @Override
    public void ack_checkpoint(Marker marker) {
        //Do something to clear past state. (optional)
        success = true;//I can emit next marker.
        if (enable_log) LOG.trace("task_size: " + epoch_size * NUM_ACCESSES);
        long elapsed_time = System.nanoTime() - boardcast_time;//the time elapsed for the system to handle the previous epoch.
        double actual_system_throughput = epoch_size * 1E9 / elapsed_time;//events/ s
        if (epoch_size != 0)
            if (enable_log) LOG.info("finished measurement (k events/s):" + actual_system_throughput / 1E3);
    }

    @Override
    public void cleanup() {
    }
}
