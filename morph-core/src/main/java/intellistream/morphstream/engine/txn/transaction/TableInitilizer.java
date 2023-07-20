package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.db.Database;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.util.FastZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.engine.txn.profiler.Metrics.NUM_ITEMS;

public abstract class TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TableInitilizer.class);
    public final Database db;

    public final double theta;
    public final int tthread;
    public final Configuration config;
    public final String split_exp = ";";
    public int floor_interval;
    public long[] p_bid;//used for partition.
    public transient FastZipfGenerator p_generator;
    public int number_partitions;
    public boolean[] multi_partion_decision;
    public int j = 0;
    public int p;
    //dual-decision
    public transient int[] dual_decision = new int[]{0, 0, 0, 0, 1, 1, 1, 1};//1:1 deposite and transfer;
    private int i = 0;

    public TableInitilizer(Database db, double theta, int tthread, Configuration config) {
        this.db = db;
        this.theta = theta;
        this.tthread = tthread;
        this.config = config;
//        State.initilize(config);
        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);
        this.number_partitions = Math.min(tthread, config.getInt("number_partitions"));
        if (ratio_of_multi_partition == 0) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, false, false};// all single.
        } else if (ratio_of_multi_partition == 0.125) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, false, true};//75% single, 25% multi.
        } else if (ratio_of_multi_partition == 0.25) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, true, true};//75% single, 25% multi.
        } else if (ratio_of_multi_partition == 0.5) {
            multi_partion_decision = new boolean[]{false, false, false, false, true, true, true, true};//equal ratio.
        } else if (ratio_of_multi_partition == 0.75) {
            multi_partion_decision = new boolean[]{false, false, true, true, true, true, true, true};//25% single, 75% multi.
        } else if (ratio_of_multi_partition == 0.875) {
            multi_partion_decision = new boolean[]{false, true, true, true, true, true, true, true};//25% single, 75% multi.
        } else if (ratio_of_multi_partition == 1) {
            multi_partion_decision = new boolean[]{true, true, true, true, true, true, true, true};// all multi.
        } else {
            throw new UnsupportedOperationException();
        }
        if (enable_log)
            LOG.info("ratio_of_multi_partition: " + ratio_of_multi_partition + "\tDECISIONS: " + Arrays.toString(multi_partion_decision));
        p_bid = new long[tthread];
        for (int i = 0; i < tthread; i++) {
            p_bid[i] = 0;
        }
        floor_interval = (int) Math.floor(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
        p_generator = new FastZipfGenerator(NUM_ITEMS, theta, 0);
    }

    protected int next_decision2() {
        int rt = dual_decision[i];
        i++;
        if (i == 8)
            i = 0;
        return rt;
    }

    public void loadDB(int maxContestants, String contestants) {
        throw new UnsupportedOperationException();
    }

    public abstract void creates_Table(Configuration config);

    public abstract void loadDB(int thread_id, int NUMTasks);

    public abstract void loadDB(int thread_id, SpinLock[] spinlock, int NUMTasks);

    public abstract void loadDB(SchedulerContext context, int thread_id, int NUMTasks);

    public abstract void loadDB(SchedulerContext context, int thread_id, SpinLock[] spinlock, int NUMTasks);

    public int get_pid(int partition_interval, int key) {
        return (int) Math.floor(key / (double) partition_interval);//NUM_ITEMS / tthread;
    }

    public abstract boolean Generate() throws IOException;

    public void prepare_input_events(int total_events) throws IOException {
        db.getEventManager().ini(total_events);
        //try to read from file or use user-defined generator for data preparation
        Generate();
        Load();
        db.getEventManager().clear();
    }

    protected abstract void Load() throws IOException;

    public abstract void store(String file_path) throws IOException;

    public abstract List<String> getTranToDecisionConf();

}
