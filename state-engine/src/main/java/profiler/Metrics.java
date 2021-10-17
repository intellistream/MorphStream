package profiler;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import static content.common.CommonMetaTypes.kMaxThreadNum;

public class Metrics {
    public static int COMPUTE_COMPLEXITY = 10;//default setting. 1, 10, 100
    public static int POST_COMPUTE_COMPLEXITY = 1;
    //change to 3 for S_STORE testing.
    public static int NUM_ACCESSES = 3;//10 as default setting. 2 for short transaction, 10 for long transaction.? --> this is the setting used in YingJun's work. 16 is the default value_list used in 1000core machine.
    public static int NUM_ITEMS = 1_000_000;//1. 1_000_000; 2. ? ; 3. 1_000  //1_000_000 YCSB has 16 million records, Ledger use 200 million records.
    public static int H2_SIZE;

    public static void RESET_COUNTERS(int thread_id) {
        //reset accumulative counters.
        Runtime.Prepare[thread_id] = 0;
    }

    public static void COMPUTE_START_TIME(int thread_id) {
        Runtime.Start[thread_id] = System.nanoTime();
    }

    public static void RECORD_SCHEDULE_TIME(int thread_id, int num_events) {
        double explore_time = Scheduler.Explore[thread_id] / (double) num_events;
        double next_time = Scheduler.Next[thread_id] / (double) num_events;
        double useful_time = Scheduler.Useful[thread_id] / (double) num_events;
        double construct_time = Scheduler.Construct[thread_id] / (double) num_events;
        double notify_time = Scheduler.Notify[thread_id] / (double) num_events;
        double first_explore_time = Scheduler.FirstExplore[thread_id] / (double) num_events;
        double caching_time = Scheduler.Caching[thread_id] / (double) num_events;
        Scheduler_Record.Explore[thread_id].addValue(explore_time);
        Scheduler_Record.Next[thread_id].addValue(next_time);
        Scheduler_Record.Useful[thread_id].addValue(useful_time);
        Scheduler_Record.Construct[thread_id].addValue(construct_time);
        Scheduler_Record.Noitfy[thread_id].addValue(notify_time);
        Scheduler_Record.FirstExplore[thread_id].addValue(first_explore_time);
        Scheduler_Record.Caching[thread_id].addValue(caching_time);
    }

    public static void RECORD_TIME(int thread_id) {
        long total_time = System.nanoTime() - Runtime.Start[thread_id];
        Total_Record.totalProcessTimePerEvent[thread_id].addValue(total_time);
        Total_Record.stream_total[thread_id].addValue(Runtime.Prepare[thread_id] + Runtime.Post[thread_id]);
        Total_Record.txn_total[thread_id].addValue(Runtime.Txn[thread_id]);
        Total_Record.overhead_total[thread_id].addValue(total_time - Runtime.Prepare[thread_id] - Runtime.Post[thread_id] - Runtime.Txn[thread_id]);
    }

    public static void RECORD_TIME(int thread_id, int number_events) {
        double total_time = (System.nanoTime() - Runtime.Start[thread_id]) / (double) number_events;
        double stream_total = (Runtime.Prepare[thread_id] + Runtime.Post[thread_id] + Runtime.PreTxn[thread_id]) / (double) number_events;
        double txn_total = Runtime.Txn[thread_id] / (double) number_events;
        Total_Record.totalProcessTimePerEvent[thread_id].addValue(total_time);
        Total_Record.stream_total[thread_id].addValue(stream_total);
        Total_Record.txn_total[thread_id].addValue(txn_total);
        Total_Record.overhead_total[thread_id].addValue(total_time - stream_total - txn_total);
    }

    public static void COMPUTE_PRE_EXE_START_TIME(int thread_id) {
        Metrics.Runtime.PrepareStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_PRE_EXE_TIME(int thread_id) {
        Runtime.Prepare[thread_id] = System.nanoTime() - Runtime.PrepareStart[thread_id];
    }

    public static void COMPUTE_PRE_EXE_TIME_ACC(int thread_id) {
        Runtime.Prepare[thread_id] += System.nanoTime() - Runtime.PrepareStart[thread_id];
    }

    public static void COMPUTE_START_INDEX_TIME(int thread_id) {
        TxnRuntime.IndexStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_INDEX_TIME_ACC(int thread_id) {
        TxnRuntime.Index[thread_id] = System.nanoTime() - TxnRuntime.IndexStart[thread_id];
    }

    public static void COMPUTE_START_POST_EXE_TIME(int thread_id) {
        Runtime.PostStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_POST_EXE_TIME(int thread_id) {
        Runtime.Post[thread_id] = System.nanoTime() - Runtime.PostStart[thread_id];
    }

    public static void COMPUTE_POST_EXE_TIME_ACC(int thread_id) {
        Runtime.Post[thread_id] += System.nanoTime() - Runtime.PostStart[thread_id];
    }

    public static void COMPUTE_START_WAIT_TIME(int thread_id) {

        TxnRuntime.WaitStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_START_LOCK_TIME(int thread_id) {

        TxnRuntime.LockStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_LOCK_TIME(int thread_id) {
        TxnRuntime.Lock[thread_id] = System.nanoTime() - TxnRuntime.LockStart[thread_id];
    }

    public static void COMPUTE_WAIT_TIME(int thread_id) {
        TxnRuntime.Wait[thread_id] = System.nanoTime() - TxnRuntime.WaitStart[thread_id] - TxnRuntime.Lock[thread_id];
    }

    public static void COMPUTE_ABORT_START_TIME(int thread_id) {

        TxnRuntime.AbortStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_ABORT_TIME_ACC(int thread_id) {
        TxnRuntime.Abort[thread_id] += System.nanoTime() - TxnRuntime.AbortStart[thread_id];
    }

    public static void COMPUTE_START_ACCESS_TIME(int thread_id) {

        TxnRuntime.AccessStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_ACCESS_TIME(int thread_id, int read_size, double write_time) {
        if (read_size == 0) {
            TxnRuntime.Access[thread_id] = (long) write_time;
        } else {
            TxnRuntime.Access[thread_id] = (System.nanoTime() - TxnRuntime.AccessStart[thread_id]) + (long) write_time;
        }
    }

    public static void COMPUTE_ACCESS_TIME_ACC(int thread_id) {
        TxnRuntime.Access[thread_id] = System.nanoTime() - TxnRuntime.AccessStart[thread_id];
    }

    public static void COMPUTE_TXN_START_TIME(int thread_id) {
        Runtime.TxnStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_TXN_TIME(int thread_id) {
        Runtime.Txn[thread_id] = (System.nanoTime() - Runtime.TxnStart[thread_id]);
    }

    public static void COMPUTE_PRE_TXN_START_TIME(int thread_id) {
        Runtime.PreTxnStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_PRE_TXN_TIME_ACC(int thread_id) {
        Runtime.PreTxn[thread_id] += System.nanoTime() - Runtime.PreTxnStart[thread_id];
    }

    public static void RECORD_TXN_BREAKDOWN_RATIO(int thread_id) {
        Transaction_Record.index_ratio[thread_id].addValue(TxnRuntime.Index[thread_id]);
        Transaction_Record.useful_ratio[thread_id].addValue(TxnRuntime.Access[thread_id]);
        Transaction_Record.lock_ratio[thread_id].addValue(TxnRuntime.Lock[thread_id]);
        Transaction_Record.sync_ratio[thread_id].addValue(TxnRuntime.Wait[thread_id]);
//        Transaction_Record.index_ratio[thread_id].addValue(TxnRuntime.Index[thread_id] / (double) Runtime.Txn[thread_id]);
//        Transaction_Record.useful_ratio[thread_id].addValue(TxnRuntime.Access[thread_id] / (double) Runtime.Txn[thread_id]);
//        Transaction_Record.lock_ratio[thread_id].addValue(TxnRuntime.Lock[thread_id] / (double) Runtime.Txn[thread_id]);
//        Transaction_Record.sync_ratio[thread_id].addValue(TxnRuntime.Wait[thread_id] / (double) Runtime.Txn[thread_id]);
    }

    // OCScheduler
    public static void COMPUTE_SCHEDULE_NEXT_START(int thread_id) {
        Scheduler.NextStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_SCHEDULE_NEXT_ACC(int thread_id) {
        Scheduler.Next[thread_id] += System.nanoTime() - Scheduler.NextStart[thread_id];
    }

    public static void COMPUTE_SCHEDULE_EXPLORE_START(int thread_id) {
        Scheduler.ExploreStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_SCHEDULE_EXPLORE_ACC(int thread_id) {
        Scheduler.Explore[thread_id] += System.nanoTime() - Scheduler.ExploreStart[thread_id];
    }

    public static void COMPUTE_SCHEDULE_USEFUL_START(int thread_id) {
        Scheduler.UsefulStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_SCHEDULE_USEFUL(int thread_id) {
        Scheduler.Useful[thread_id] += System.nanoTime() - Scheduler.UsefulStart[thread_id];
    }

    public static void COMPUTE_CONSTRUCT_START(int thread_id) {
        Scheduler.ConstructStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_CONSTRUCT(int thread_id) {
        Scheduler.Construct[thread_id] += System.nanoTime() - Scheduler.ConstructStart[thread_id];
    }

    public static void COMPUTE_CACHE_OPERATION_START(int thread_id) {
        Scheduler.CachingStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_CACHE_OPERATION(int thread_id) {
        Scheduler.Caching[thread_id] += System.nanoTime() - Scheduler.CachingStart[thread_id];
    }

    public static void COMPUTE_FIRST_EXPLORE_START(int thread_id) {
        Scheduler.FirstExploreStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_FIRST_EXPLORE(int thread_id) {
        Scheduler.FirstExplore[thread_id] += System.nanoTime() - Scheduler.FirstExploreStart[thread_id];
    }

    public static void COMPUTE_NOTIFY_START(int thread_id) {
        Scheduler.NotifyStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_NOTIFY(int thread_id) {
        Scheduler.Notify[thread_id] += System.nanoTime() - Scheduler.NotifyStart[thread_id];
    }

    static class TxnRuntime {
        public static long[] IndexStart = new long[kMaxThreadNum];
        public static long[] Index = new long[kMaxThreadNum];
        public static long[] WaitStart = new long[kMaxThreadNum];
        public static long[] Wait = new long[kMaxThreadNum];
        public static long[] LockStart = new long[kMaxThreadNum];
        public static long[] Lock = new long[kMaxThreadNum];
        public static long[] AccessStart = new long[kMaxThreadNum];
        public static long[] Access = new long[kMaxThreadNum];
        public static long[] AbortStart = new long[kMaxThreadNum];
        public static long[] Abort = new long[kMaxThreadNum];

        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                IndexStart[i] = 0;
                Index[i] = 0;
                WaitStart[i] = 0;
                LockStart[i] = 0;
                Lock[i] = 0;
                Wait[i] = 0;
                AccessStart[i] = 0;
                Access[i] = 0;
                AbortStart[i] = 0;
                Abort[i] = 0;
            }
        }
    }

    static class Runtime {
        public static long[] Start = new long[kMaxThreadNum];
        public static long[] PrepareStart = new long[kMaxThreadNum];
        public static long[] Prepare = new long[kMaxThreadNum];
        public static long[] PostStart = new long[kMaxThreadNum];
        public static long[] Post = new long[kMaxThreadNum];
        public static long[] TxnStart = new long[kMaxThreadNum];
        public static long[] Txn = new long[kMaxThreadNum];
        //USED ONLY BY TStream
        public static long[] PreTxnStart = new long[kMaxThreadNum];
        public static long[] PreTxn = new long[kMaxThreadNum];

        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                Start[i] = 0;
                PrepareStart[i] = 0;
                Prepare[i] = 0;
                PostStart[i] = 0;
                Post[i] = 0;
                TxnStart[i] = 0;
                Txn[i] = 0;
                PreTxnStart[i] = 0;
                PreTxn[i] = 0;
            }
        }
    }

    static class Total_Record {
        public static DescriptiveStatistics[] totalProcessTimePerEvent = new DescriptiveStatistics[kMaxThreadNum];//total time spend for every input event.
        public static DescriptiveStatistics[] txn_total = new DescriptiveStatistics[kMaxThreadNum];//total time spend in txn.
        public static DescriptiveStatistics[] stream_total = new DescriptiveStatistics[kMaxThreadNum];//total time spend in stream processing.
        public static DescriptiveStatistics[] overhead_total = new DescriptiveStatistics[kMaxThreadNum];//other overheads.

        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                totalProcessTimePerEvent[i] = new DescriptiveStatistics();
                txn_total[i] = new DescriptiveStatistics();
                stream_total[i] = new DescriptiveStatistics();
                overhead_total[i] = new DescriptiveStatistics();
            }
        }
    }

    static class Transaction_Record {
        public static DescriptiveStatistics[] index_ratio = new DescriptiveStatistics[kMaxThreadNum];//index
        public static DescriptiveStatistics[] useful_ratio = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.
        public static DescriptiveStatistics[] sync_ratio = new DescriptiveStatistics[kMaxThreadNum];// sync_ratio lock_ratio and order.
        public static DescriptiveStatistics[] lock_ratio = new DescriptiveStatistics[kMaxThreadNum];// sync_ratio lock_ratio and order.

        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                index_ratio[i] = new DescriptiveStatistics();
                useful_ratio[i] = new DescriptiveStatistics();
                sync_ratio[i] = new DescriptiveStatistics();
                lock_ratio[i] = new DescriptiveStatistics();
            }
        }
    }

    static class Scheduler {
        public static long[] NextStart = new long[kMaxThreadNum];
        public static long[] Next = new long[kMaxThreadNum];
        public static long[] UsefulStart = new long[kMaxThreadNum];
        public static long[] Useful = new long[kMaxThreadNum];
        public static long[] Explore = new long[kMaxThreadNum];
        public static long[] ExploreStart = new long[kMaxThreadNum];
        public static long[] ConstructStart = new long[kMaxThreadNum];
        public static long[] Construct = new long[kMaxThreadNum];
        public static long[] NotifyStart = new long[kMaxThreadNum];
        public static long[] Notify = new long[kMaxThreadNum];
        public static long[] FirstExploreStart = new long[kMaxThreadNum];
        public static long[] FirstExplore = new long[kMaxThreadNum];
        public static long[] CachingStart = new long[kMaxThreadNum];
        public static long[] Caching = new long[kMaxThreadNum];


        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                NextStart[i] = 0;
                Next[i] = 0;
                UsefulStart[i] = 0;
                Useful[i] = 0;
                ExploreStart[i] = 0;
                Explore[i] = 0;
                ConstructStart[i] = 0;
                Construct[i] = 0;
                NotifyStart[i] = 0;
                Notify[i] = 0;
                FirstExploreStart[i] = 0;
                FirstExplore[i] = 0;
                CachingStart[i] = 0;
                Caching[i] = 0;
            }
        }
    }

    static class Scheduler_Record {
        public static DescriptiveStatistics[] Next = new DescriptiveStatistics[kMaxThreadNum];//NEXT.
        public static DescriptiveStatistics[] Explore = new DescriptiveStatistics[kMaxThreadNum];//EXPLORE.
        public static DescriptiveStatistics[] Useful = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.
        public static DescriptiveStatistics[] Construct = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.
        public static DescriptiveStatistics[] Noitfy = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.
        public static DescriptiveStatistics[] FirstExplore = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.
        public static DescriptiveStatistics[] Caching = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.

        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                Next[i] = new DescriptiveStatistics();
                Explore[i] = new DescriptiveStatistics();
                Useful[i] = new DescriptiveStatistics();
                Construct[i] = new DescriptiveStatistics();
                Noitfy[i] = new DescriptiveStatistics();
                FirstExplore[i] = new DescriptiveStatistics();
                Caching[i] = new DescriptiveStatistics();
            }
        }
    }
}
