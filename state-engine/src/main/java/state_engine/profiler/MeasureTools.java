package state_engine.profiler;
import common.CONTROL;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import static state_engine.Meta.MetaTypes.kMaxThreadNum;
public class MeasureTools {
    //control.
    public static long[] measure_counts = new long[kMaxThreadNum];
    public static boolean[] profile_start = new boolean[kMaxThreadNum];
    protected static Metrics metrics = Metrics.getInstance();
    static long[] total_start = new long[kMaxThreadNum];
    static long[] total = new long[kMaxThreadNum];

    public static long[] number_of_ocs = new long[kMaxThreadNum];

    static long[] txn_wait_start = new long[kMaxThreadNum];
    static long[] txn_wait = new long[kMaxThreadNum];
    static long[] txn_lock_start = new long[kMaxThreadNum];
    static long[] txn_lock = new long[kMaxThreadNum];
    static long[] prepare_start = new long[kMaxThreadNum];
    static long[] prepare_time = new long[kMaxThreadNum];
    static long[] post_time_start = new long[kMaxThreadNum];
    static long[] post_time = new long[kMaxThreadNum];
    static long[] compute_end = new long[kMaxThreadNum];
    static long[] index_start = new long[kMaxThreadNum];
    static long[] index_time = new long[kMaxThreadNum];
    static long[] abort_start = new long[kMaxThreadNum];
    static long[] abort_time = new long[kMaxThreadNum];
    static long[] ts_allocate_start = new long[kMaxThreadNum];
    static long[] ts_allocate = new long[kMaxThreadNum];
    //t-stream special.
    static long[] pre_txn_start = new long[kMaxThreadNum];
    static long[] pre_txn_total = new long[kMaxThreadNum];
    static long[] create_oc_start = new long[kMaxThreadNum];
    static long[] create_oc_total = new long[kMaxThreadNum];
    static long[] dependency_checking_start = new long[kMaxThreadNum];
    static long[] dependency_checking_total = new long[kMaxThreadNum];
    static long[] dependency_outoforder_overhead_start = new long[kMaxThreadNum];
    static long[] dependency_outoforder_overhead_total = new long[kMaxThreadNum];

    static long[] write_handle_start = new long[kMaxThreadNum];
    static long[] write_handle = new long[kMaxThreadNum];

    static long[] txn_start = new long[kMaxThreadNum];
    static double[] txn_total = new double[kMaxThreadNum];
    static long[] txn_processing_start = new long[kMaxThreadNum];
    static long[] txn_processing_total = new long[kMaxThreadNum];//tp=tp_core + tp_submit
    static long[] calculate_levels_start = new long[kMaxThreadNum];
    static long[] calculate_levels_total = new long[kMaxThreadNum];
    static long[] barriers_start = new long[kMaxThreadNum];
    static long[] barriers_total = new long[kMaxThreadNum];
    static long[] iterative_processing_useful_start = new long[kMaxThreadNum];
    static long[] iterative_processing_useful_total = new long[kMaxThreadNum];
    static long[] iterative_ocs_submit_start = new long[kMaxThreadNum];
    static long[] iterative_ocs_submit_total = new long[kMaxThreadNum];
    static long[] access_start = new long[kMaxThreadNum];
    static double[] access_total = new double[kMaxThreadNum];

    static long[] db_access_start = new long[kMaxThreadNum];
    static long[] db_access_time = new long[kMaxThreadNum];

    static long[] tp_core_start = new long[kMaxThreadNum];
    static double[] tp_core = new double[kMaxThreadNum];
    static long[] tp_submit_start = new long[kMaxThreadNum];
    static double[] tp_submit = new double[kMaxThreadNum];


    public MeasureTools() {
        for (int i = 0; i < kMaxThreadNum; i++) {
            profile_start[i] = false;
            total_start[i] = 0;
            txn_lock[i] = 0;
            txn_wait[i] = 0;

            number_of_ocs[i] = 0;
            pre_txn_total[i] = 0;
            create_oc_total[i] = 0;
            dependency_checking_total[i] = 0;
            dependency_outoforder_overhead_total[i] = 0;


        }
    }
    public static void BEGIN_TOTAL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile) {
            if (measure_counts[thread_id] >= CONTROL.MeasureStart && measure_counts[thread_id] < CONTROL.MeasureBound) {
                profile_start[thread_id] = true;
                prepare_start[thread_id] = System.nanoTime();
                total_start[thread_id] = prepare_start[thread_id];
            } else
                profile_start[thread_id] = false;
        }
    }

    //compute per event time spent.
    public static void END_TOTAL_TIME_MEASURE(int thread_id, int combo_bid_size) {
        if (profile_start[thread_id] && !Thread.currentThread().isInterrupted()) {
            long overall_processing_time_per_batch = System.nanoTime() - total_start[thread_id];
            metrics.overhead_total[thread_id].addValue((overall_processing_time_per_batch - txn_total[thread_id]
                    - post_time[thread_id] - prepare_time[thread_id]) / combo_bid_size);
            metrics.stream_total[thread_id].addValue((post_time[thread_id] + prepare_time[thread_id]) / combo_bid_size);
            metrics.txn_total[thread_id].addValue(txn_total[thread_id] / combo_bid_size);
            //clean.
            access_total[thread_id] = 0;
            index_time[thread_id] = 0;
            tp_core[thread_id] = 0;
        }
        measure_counts[thread_id]++;
    }

    public static void END_PREPARE_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id]) {
            prepare_time[thread_id] = System.nanoTime() - prepare_start[thread_id];
        }
    }

    public static void BEGIN_INDEX_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            index_start[thread_id] = System.nanoTime();
    }
    public static void END_INDEX_TIME_MEASURE_ACC(int thread_id, boolean is_retry_) {
        if (profile_start[thread_id]) {
            index_time[thread_id] += System.nanoTime() - index_start[thread_id];
        }
    }

    public static void BEGIN_POST_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            post_time_start[thread_id] = System.nanoTime();
    }
    public static void END_POST_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id]) {
            post_time[thread_id] = System.nanoTime() - post_time_start[thread_id];
        }
    }
    public static void END_POST_TIME_MEASURE_ACC(int thread_id) {
        if (profile_start[thread_id]) {
            post_time[thread_id] += System.nanoTime() - post_time_start[thread_id];
        }
    }
    public static void BEGIN_WAIT_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            txn_wait_start[thread_id] = System.nanoTime();
    }
    public static void BEGIN_LOCK_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            txn_lock_start[thread_id] = System.nanoTime();
    }
    public static void END_LOCK_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id]) {
            txn_lock[thread_id] = System.nanoTime() - txn_lock_start[thread_id];
        }
    }
    public static void END_WAIT_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            txn_wait[thread_id] = (System.nanoTime() - txn_wait_start[thread_id] - txn_lock[thread_id]);
    }

    public static void END_TRANSACTION_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id] && !Thread.currentThread().isInterrupted()) {
            txn_total[thread_id] = (System.nanoTime() - txn_start[thread_id]);
//            metrics.index_ratio[thread_id].addValue(index_time[thread_id] / txn_total[thread_id]);
            metrics.useful_ratio[thread_id].addValue(access_total[thread_id] / txn_total[thread_id]);
            //should be zero under NO_LOCK.
            metrics.lock_ratio[thread_id].addValue(txn_lock[thread_id] / txn_total[thread_id]);
            metrics.sync_ratio[thread_id].addValue(txn_wait[thread_id] / txn_total[thread_id]);
        }
    }

    public static void END_ACCESS_TIME_MEASURE_ACC(int thread_id) {
        if (profile_start[thread_id]) {
            access_total[thread_id] += System.nanoTime() - access_start[thread_id];
        }
    }

    //t-stream specials
    public static void BEGIN_TOTAL_TIME_MEASURE_TS(int thread_id) {
        if (CONTROL.enable_profile) {

            if (profile_start[thread_id] || measure_counts[thread_id] >= CONTROL.MeasureStart) {

                profile_start[thread_id] = true;

                prepare_start[thread_id] = System.nanoTime();

                if (total_start[thread_id] == 0)
                    total_start[thread_id] = prepare_start[thread_id];

            } else
                profile_start[thread_id] = false;
            measure_counts[thread_id]++;
        }
    }

    public static void END_PREPARE_TIME_MEASURE_TS(int thread_id) {
        if (profile_start[thread_id]) {
            prepare_time[thread_id] += System.nanoTime() - prepare_start[thread_id];
        }
    }

    public static void BEGIN_PRE_TXN_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            pre_txn_start[thread_id] = System.nanoTime();
    }

    public static void END_PRE_TXN_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id]) {
            pre_txn_total[thread_id] += System.nanoTime() - pre_txn_start[thread_id];
        }
    }

    public static void BEGIN_CREATE_OC_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            create_oc_start[thread_id] = System.nanoTime();
    }

    public static void END_CREATE_OC_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id]) {
            create_oc_total[thread_id] += System.nanoTime() - create_oc_start[thread_id];
        }
    }

    public static void BEGIN_DEPENDENCY_CHECKING_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            dependency_checking_start[thread_id] = System.nanoTime();
    }

    public static void END_DEPENDENCY_CHECKING_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id]) {
            dependency_checking_total[thread_id] += System.nanoTime() - dependency_checking_start[thread_id];
        }
    }

    public static void BEGIN_DB_ACCESS_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            db_access_start[thread_id] = System.nanoTime();
    }

    public static void END_DB_ACCESS_TIME_MEASURE(int thread_id, boolean is_retry_) {
        if (profile_start[thread_id]) {
            db_access_time[thread_id] += System.nanoTime() - db_access_start[thread_id];
        }
    }

    public static void BEGIN_DEPENDENCY_OUTOFORDER_OVERHEAD_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            dependency_outoforder_overhead_start[thread_id] = System.nanoTime();
    }

    public static void END_DEPENDENCY_OUTOFORDER_OVERHEAD_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id]) {
            dependency_outoforder_overhead_total[thread_id] += System.nanoTime() - dependency_outoforder_overhead_start[thread_id];
        }
    }

    public static void BEGIN_BARRIER_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            barriers_start[thread_id] = System.nanoTime();
    }

    public static void END_BARRIER_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id]) {
            barriers_total[thread_id] += System.nanoTime() - barriers_start[thread_id];
        }
    }

    public static void BEGIN_TXN_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            txn_start[thread_id] = System.nanoTime();
    }

    //needs to include requests construction time /* pre_txn_total */.
    // ! Thread.interrupted() will clear the interrupt flag!! be very careful!!
    public static void END_TXN_TIME_MEASURE_TS(int thread_id, double pushdown_usefultime) {
        if (profile_start[thread_id] && !Thread.currentThread().isInterrupted()) {
//            pre_txn_total[thread_id] -= post_time[thread_id];//need to exclude write-post time.
            txn_total[thread_id] = ((System.nanoTime() - txn_start[thread_id]));

//            metrics.index_ratio[thread_id].addValue(index_time[thread_id] / txn_total[thread_id]);
            metrics.useful_ratio[thread_id].addValue((access_total[thread_id]) / txn_total[thread_id]);
            metrics.lock_ratio[thread_id].addValue(0);
            metrics.sync_ratio[thread_id].addValue((txn_processing_total[thread_id] - pushdown_usefultime) / txn_total[thread_id]);
        }
    }

    public static void BEGIN_TXN_PROCESSING_TIME_MEASURE(int thread_id) { // transaction processing time
        if (profile_start[thread_id])
            txn_processing_start[thread_id] = System.nanoTime();
    }

    public static void END_TXN_PROCESSING_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            txn_processing_total[thread_id] = System.nanoTime() - txn_processing_start[thread_id];
    }


    public static void BEGIN_CALCULATE_LEVELS_TIME_MEASURE(int thread_id) { // transaction processing time
        if (profile_start[thread_id])
            calculate_levels_start[thread_id] = System.nanoTime();
    }

    public static void END_CALCULATE_LEVELS_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            calculate_levels_total[thread_id] = System.nanoTime() - calculate_levels_start[thread_id];
    }

    public static void BEGIN_ITERATIVE_OCS_SUBMIT_TIME_MEASURE(int thread_id) { // transaction processing time
        if (profile_start[thread_id])
            iterative_ocs_submit_start[thread_id] = System.nanoTime();
    }

    public static void END_ITERATIVE_OCS_SUBMIT_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            iterative_ocs_submit_total[thread_id] += System.nanoTime() - iterative_ocs_submit_start[thread_id];
    }

    public static void BEGIN_ITERATIVE_PROCESSING_USEFUL_TIME_MEASURE(int thread_id) { // transaction processing time
        if (profile_start[thread_id])
            iterative_processing_useful_start[thread_id] = System.nanoTime();
    }

    public static void END_ITERATIVE_PROCESSING_USEFUL_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            iterative_processing_useful_total[thread_id] += System.nanoTime() - iterative_processing_useful_start[thread_id];
    }

    public static void BEGIN_ACCESS_TIME_MEASURE(int thread_id) {
        if (profile_start[thread_id])
            access_start[thread_id] = System.nanoTime();
    }

    //needs to include write compute time also for TS.
    public static void END_ACCESS_TIME_MEASURE_TS(int thread_id, int read_size, double write_useful_time, int write_size) {
        if (profile_start[thread_id]) {
            if (read_size == 0) {
                access_total[thread_id] = (write_useful_time * write_size);
            } else {
                access_total[thread_id] = (System.nanoTime() - access_start[thread_id]) + (write_useful_time * write_size);
            }
        }
    }

    public static void REGISTER_NUMBER_OF_OC_PROCESSED(int thread, int noOfOcs){
        number_of_ocs[thread] = noOfOcs;
    }

    //compute per event time spent.
    public static void END_TOTAL_TIME_MEASURE_TS(int thread_id, int numberOfTransactionalEvents) {


        if (profile_start[thread_id] && !Thread.currentThread().isInterrupted()) {

            total[thread_id] = System.nanoTime() - total_start[thread_id];//time from receiving first event to finish last event in the current batch.

            metrics.numberOf_transactional_events_processed[thread_id].addValue(numberOfTransactionalEvents);
            numberOfTransactionalEvents = 1; // Do not scale time to time per transaction.

            metrics.number_of_ocs_processed[thread_id].addValue(number_of_ocs[thread_id]);

            metrics.overhead_total[thread_id].addValue((total[thread_id] - txn_total[thread_id]) / numberOfTransactionalEvents);
            metrics.stream_total[thread_id].addValue((post_time[thread_id] + prepare_time[thread_id]) / numberOfTransactionalEvents);

            metrics.total[thread_id].addValue(total[thread_id] / numberOfTransactionalEvents);

            metrics.txn_total[thread_id].addValue(txn_total[thread_id] / numberOfTransactionalEvents);
            metrics.txn_processing_total[thread_id].addValue(txn_processing_total[thread_id] / numberOfTransactionalEvents);
            metrics.state_access_total[thread_id].addValue(access_total[thread_id] / numberOfTransactionalEvents);
            metrics.calculate_levels_total[thread_id].addValue(calculate_levels_total[thread_id] / numberOfTransactionalEvents);
            metrics.barriers_total[thread_id].addValue(barriers_total[thread_id] / numberOfTransactionalEvents);
            metrics.iterative_processing_useful_total[thread_id].addValue(iterative_processing_useful_total[thread_id] / numberOfTransactionalEvents);

            metrics.pre_txn_total[thread_id].addValue(pre_txn_total[thread_id] / numberOfTransactionalEvents);
            metrics.create_oc_total[thread_id].addValue(create_oc_total[thread_id] / numberOfTransactionalEvents);
            metrics.dependency_checking_total[thread_id].addValue(dependency_checking_total[thread_id] / numberOfTransactionalEvents);
            metrics.dependency_outoforder_overhead_total[thread_id].addValue(dependency_outoforder_overhead_total[thread_id] / numberOfTransactionalEvents);
            metrics.db_access_time[thread_id].addValue(db_access_time[thread_id] / numberOfTransactionalEvents);

//            metrics.average_tp_core[thread_id].addValue(tp_core[thread_id] / numberOfTransactionalEvents);
//            metrics.average_tp_submit[thread_id].addValue(tp_submit[thread_id] / numberOfTransactionalEvents);
//            metrics.average_txn_construct[thread_id].addValue((double) pre_txn_total[thread_id] / numberOfTransactionalEvents);
//            metrics.average_tp_w_syn[thread_id].addValue((double) tp[thread_id] / numberOfTransactionalEvents);
            //clean;

            barriers_total[thread_id]=0;
            total_start[thread_id] = 0;
            total[thread_id] = 0;
            access_total[thread_id] = 0;
            index_time[thread_id] = 0;

            txn_total[thread_id] = 0;
            txn_processing_total[thread_id] = 0;
            access_total[thread_id] = 0;
            calculate_levels_total[thread_id] = 0;
            iterative_processing_useful_total[thread_id] = 0;

            pre_txn_total[thread_id] = 0;
            create_oc_total[thread_id] = 0;
            dependency_checking_total[thread_id] = 0;
            dependency_outoforder_overhead_total[thread_id] = 0;
            db_access_time[thread_id] = 0;

            write_handle[thread_id] = 0;
            prepare_time[thread_id] = 0;
            tp_core[thread_id] = 0;
            tp_submit[thread_id] = 0;
            post_time[thread_id] = 0;

        }
    }
    //t-stream specials end here.

    public static void END_LOCK_TIME_MEASURE_NOCC(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
            long rt = System.nanoTime() - txn_lock_start[thread_id];
            txn_lock[thread_id] += rt - tp_core[thread_id] - index_time[thread_id];
        }
    }
        public static void END_COMPUTE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
            access_total[thread_id] = System.nanoTime() - access_start[thread_id];
        }
    }
    public static void END_INDEX_TIME_MEASURE(int thread_id, boolean is_retry_) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
            if (!is_retry_)
                index_time[thread_id] = System.nanoTime() - index_start[thread_id];
        }
    }
    public static void BEGIN_ABORT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            abort_start[thread_id] = System.nanoTime();
    }
    public static void END_ABORT_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            abort_time[thread_id] += System.nanoTime() - abort_start[thread_id];
    }
    public static void CLEAN_ABORT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            abort_time[thread_id] = 0;
    }
    public static void BEGIN_TS_ALLOCATE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            ts_allocate_start[thread_id] = System.nanoTime();
    }
    public static void END_TS_ALLOCATE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            ts_allocate[thread_id] = System.nanoTime() - ts_allocate_start[thread_id];
    }
    public static void BEGIN_WRITE_HANDLE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            write_handle_start[thread_id] = System.nanoTime();
    }
    public static void END_WRITE_HANDLE_TIME_MEASURE_TS(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            write_handle[thread_id] += System.nanoTime() - write_handle_start[thread_id];
    }
    public static void BEGIN_TP_SUBMIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            tp_submit_start[thread_id] = System.nanoTime();
    }
    public static void END_TP_SUBMIT_TIME_MEASURE(int thread_id, int size) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
            tp_submit[thread_id] = (double) (System.nanoTime() - tp_submit_start[thread_id]);
        }
    }
    public static void BEGIN_TP_CORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
            tp_core_start[thread_id] = System.nanoTime();
    }
    public static void END_TP_CORE_TIME_MEASURE_TS(int thread_id, int size) {
        if (CONTROL.enable_profile && CONTROL.MeasureStart <= measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
            tp_core[thread_id] = (System.nanoTime() - tp_core_start[thread_id] - tp_submit[thread_id]);
        }
    }
}
