package intellistream.morphstream.engine.txn.profiler;

import intellistream.morphstream.common.io.LocalFS.FileSystem;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import java.io.File;
import java.util.*;

import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.kMaxThreadNum;

public class Metrics {
    public static final DescriptiveStatistics usedMemory = new DescriptiveStatistics();
    public static final DescriptiveStatistics SSDBandwidth = new DescriptiveStatistics();
    public static final DescriptiveStatistics usedFileSize = new DescriptiveStatistics();
    public static int COMPUTE_COMPLEXITY = 10;//default setting. 1, 10, 100
    public static int POST_COMPUTE_COMPLEXITY = 1;
    //change to 3 for S_STORE testing.
    public static int NUM_ACCESSES = 3;//10 as default setting. 2 for short transaction, 10 for long transaction.? --> this is the setting used in YingJun's work. 16 is the default value_list used in 1000core machine.
    public static int NUM_ITEMS = 500_000;//1. 1_000_000; 2. ? ; 3. 1_000  //1_000_000 YCSB has 16 million records, Ledger use 200 million records.
    public static int H2_SIZE;
    public static Timer timer = new Timer();
    public static String directory;
    public static String fileNameSuffix;

    public static void RESET_COUNTERS(int thread_id) {
        //reset accumulative counters.
        Runtime.Prepare[thread_id] = 0;
        Runtime.PreTxn[thread_id] = 0;
    }

    public static void COMPUTE_START_TIME(int thread_id) {
        Runtime.Start[thread_id] = System.nanoTime();
    }

    public static void RECORD_SCHEDULE_TIME(int thread_id, int num_events) {
        if (!RecoveryPerformance.stopRecovery[thread_id]) {
            RecoveryPerformance.Explore[thread_id] = RecoveryPerformance.Explore[thread_id] + Scheduler.Explore[thread_id] - Scheduler.Useful[thread_id] - Scheduler.Abort[thread_id] - Scheduler.Wait[thread_id];
            RecoveryPerformance.Useful[thread_id] = RecoveryPerformance.Useful[thread_id] + (Scheduler.Useful[thread_id] * (Scheduler.UsefulCount[thread_id] - Scheduler.RedoCount[thread_id]) / Scheduler.UsefulCount[thread_id]);
            RecoveryPerformance.Abort[thread_id] = RecoveryPerformance.Abort[thread_id] + (Scheduler.Abort[thread_id] + Scheduler.Useful[thread_id] * Scheduler.RedoCount[thread_id] / Scheduler.UsefulCount[thread_id]);
            RecoveryPerformance.Construct[thread_id] = RecoveryPerformance.Construct[thread_id] + Scheduler.Construct[thread_id];
            RecoveryPerformance.Wait[thread_id] = RecoveryPerformance.Wait[thread_id] + Scheduler.Wait[thread_id];
        } else {
            double explore_time = (Scheduler.Explore[thread_id] - Scheduler.Useful[thread_id] - Scheduler.Abort[thread_id] - Scheduler.Tracking[thread_id] - Scheduler.Wait[thread_id]) / (double) num_events;
            double useful_time = (Scheduler.Useful[thread_id] * (Scheduler.UsefulCount[thread_id] - Scheduler.RedoCount[thread_id]) / (double) Scheduler.UsefulCount[thread_id]) / (double) num_events;
            double abort_time = (Scheduler.Abort[thread_id] + Scheduler.Useful[thread_id] * Scheduler.RedoCount[thread_id] / (double) Scheduler.UsefulCount[thread_id]) / (double) num_events;
            double construct_time = Scheduler.Construct[thread_id] / (double) num_events;
            double tracking_time = Scheduler.Tracking[thread_id] / (double) num_events;
            double wait_time = Scheduler.Wait[thread_id] / (double) num_events;
            Scheduler_Record.Explore[thread_id].addValue(explore_time);
            Scheduler_Record.Useful[thread_id].addValue(useful_time);
            Scheduler_Record.Abort[thread_id].addValue(abort_time);
            Scheduler_Record.Construct[thread_id].addValue(construct_time);
            Scheduler_Record.Tracking[thread_id].addValue(tracking_time);
            Scheduler_Record.Wait[thread_id].addValue(wait_time);
        }
        Scheduler.Initialize(thread_id);
    }

    public static void RECORD_TIME(int thread_id) {
        long total_time = System.nanoTime() - Runtime.Start[thread_id];
        Total_Record.totalProcessTimePerEvent[thread_id].addValue(total_time);
        Total_Record.stream_total[thread_id].addValue(Runtime.Prepare[thread_id] + Runtime.Post[thread_id]);
        Total_Record.txn_total[thread_id].addValue(Runtime.Txn[thread_id]);
        Total_Record.overhead_total[thread_id].addValue(total_time - Runtime.Prepare[thread_id] - Runtime.Post[thread_id] - Runtime.Txn[thread_id]);
    }

    public static void RECORD_TIME(int thread_id, int number_events) {
        if (!RecoveryPerformance.stopRecovery[thread_id]) {
            long total_time = System.nanoTime() - Runtime.Start[thread_id];
            long txn_total = Runtime.Txn[thread_id];
            long stream_total = Runtime.Prepare[thread_id] + Runtime.Post[thread_id] + Runtime.PreTxn[thread_id];
            long overhead_total = total_time - txn_total - stream_total;
            RecoveryPerformance.total_time[thread_id] = RecoveryPerformance.total_time[thread_id] + total_time;
            RecoveryPerformance.txn_total[thread_id] = RecoveryPerformance.txn_total[thread_id] + txn_total;
            RecoveryPerformance.stream_total[thread_id] = RecoveryPerformance.stream_total[thread_id] + stream_total;
            RecoveryPerformance.overhead_total[thread_id] = RecoveryPerformance.overhead_total[thread_id] + overhead_total;
        } else {
            //FaultTolerance
            double compression_total = Runtime.Compression[thread_id] / (double) number_events;
            double persist_total = Runtime.Persist[thread_id] / (double) number_events;
            double logging_total = Runtime.Logging[thread_id] / (double) number_events;
            double snapshot_total = Runtime.Snapshot[thread_id] / (double) number_events;
            Total_Record.compression_total[thread_id].addValue(compression_total);
            Total_Record.persist_total[thread_id].addValue(persist_total);
            Total_Record.serialization_total[thread_id].addValue(logging_total);
            Total_Record.snapshot_serialization_total[thread_id].addValue(snapshot_total);
            logging_total = logging_total + snapshot_total;
            Runtime.Snapshot[thread_id] = 0;//Need to reset it to 0
            //Process
            double total_process_time = (System.nanoTime() - Runtime.Start[thread_id]) / (double) number_events;
            double stream_total = (Runtime.Prepare[thread_id] + Runtime.Post[thread_id] + Runtime.PreTxn[thread_id]) / (double) number_events;
            double txn_total = Runtime.Txn[thread_id] / (double) number_events;
            Total_Record.totalProcessTimePerEvent[thread_id].addValue(total_process_time - logging_total);
            Total_Record.stream_total[thread_id].addValue(stream_total);
            Total_Record.txn_total[thread_id].addValue(txn_total);
            Total_Record.overhead_total[thread_id].addValue(total_process_time - stream_total - txn_total - logging_total);
            Runtime.ThroughputPerPhase.get(thread_id).add(1 / total_process_time);
        }
        Runtime.Start[thread_id] = 0;
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
        TxnRuntime.Index[thread_id] += System.nanoTime() - TxnRuntime.IndexStart[thread_id];
    }

    public static void COMPUTE_START_POST_EXE_TIME(int thread_id) {
        Runtime.PostStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_POST_EXE_TIME(int thread_id) {
        Runtime.Post[thread_id] = System.nanoTime() - Runtime.PostStart[thread_id];
    }

    public static void COMPUTE_POST_EXE_TIME_ACC(int thread_id) {
        Runtime.Post[thread_id] = System.nanoTime() - Runtime.PostStart[thread_id];
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

    public static void COMPUTE_LOCK_TIME_ACC(int thread_id) {
        TxnRuntime.Lock[thread_id] += System.nanoTime() - TxnRuntime.LockStart[thread_id];
    }

    public static void COMPUTE_WAIT_TIME(int thread_id) {
        TxnRuntime.Wait[thread_id] = System.nanoTime() - TxnRuntime.WaitStart[thread_id] - TxnRuntime.Lock[thread_id];
    }

    public static void COMPUTE_WAIT_TIME_ACC(int thread_id) {
//        TxnRuntime.Wait[thread_id] += System.nanoTime() - TxnRuntime.WaitStart[thread_id] - TxnRuntime.Lock[thread_id];
        TxnRuntime.Wait[thread_id] += System.nanoTime() - TxnRuntime.WaitStart[thread_id];
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
        TxnRuntime.Access[thread_id] += System.nanoTime() - TxnRuntime.AccessStart[thread_id];
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

    //Fault Tolerance Specific
    public static void COMPUTE_COMPRESSION_START_TIME(int thread_id) {
        Runtime.CompressionStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_COMPRESSION_TIME(int thread_id) {
        Runtime.Compression[thread_id] = System.nanoTime() - Runtime.CompressionStart[thread_id];
    }

    public static void COMPUTE_PERSIST_START_TIME(int thread_id) {
        Runtime.PersistStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_PERSIST_TIME(int thread_id) {
        Runtime.Persist[thread_id] = System.nanoTime() - Runtime.PersistStart[thread_id];
    }

    public static void COMPUTE_SNAPSHOT_START_TIME(int thread_id) {
        Runtime.SnapshotStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_SNAPSHOT_TIME(int thread_id) {
        Runtime.Snapshot[thread_id] = System.nanoTime() - Runtime.SnapshotStart[thread_id];
    }

    public static void COMPUTE_LOGGING_START_TIME(int thread_id) {
        Runtime.LoggingStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_LOGGING_TIME(int thread_id) {
        Runtime.Logging[thread_id] = System.nanoTime() - Runtime.LoggingStart[thread_id];
    }

    public static void RECORD_TXN_BREAKDOWN_RATIO(int thread_id) {
        Transaction_Record.index_ratio[thread_id].addValue(TxnRuntime.Index[thread_id]);
        Transaction_Record.useful_ratio[thread_id].addValue(TxnRuntime.Access[thread_id]);
        Transaction_Record.lock_ratio[thread_id].addValue(TxnRuntime.Lock[thread_id]);
        Transaction_Record.sync_ratio[thread_id].addValue(TxnRuntime.Wait[thread_id]);
        TxnRuntime.Initialize(thread_id);
    }

    public static void RECORD_TXN_BREAKDOWN_RATIO(int thread_id, int number_events) {
        Transaction_Record.index_ratio[thread_id].addValue(TxnRuntime.Index[thread_id] / (double) number_events);
        Transaction_Record.useful_ratio[thread_id].addValue(TxnRuntime.Access[thread_id] / (double) number_events);
        Transaction_Record.lock_ratio[thread_id].addValue(TxnRuntime.Lock[thread_id] / (double) number_events);
        Transaction_Record.sync_ratio[thread_id].addValue(TxnRuntime.Wait[thread_id] / (double) number_events);
        TxnRuntime.Initialize(thread_id);
    }

    // OGScheduler
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
        Scheduler.UsefulCount[thread_id]++;
    }

    public static void COMPUTE_SCHEDULE_TRACKING_START(int thread_id) {
        Scheduler.TrackingStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_SCHEDULE_TRACKING(int thread_id) {
        Scheduler.Tracking[thread_id] += System.nanoTime() - Scheduler.TrackingStart[thread_id];
    }

    public static void COMPUTE_SCHEDULE_WAIT_START(int thread_id) {
        Scheduler.WaitStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_SCHEDULE_WAIT(int thread_id) {
        Scheduler.Wait[thread_id] += System.nanoTime() - Scheduler.WaitStart[thread_id];
    }

    public static void COMPUTE_SCHEDULE_ABORT_START(int thread_id) {
        Scheduler.isAbort[thread_id] = true;
        Scheduler.AbortStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_SCHEDULE_ABORT(int thread_id) {
        if (Scheduler.isAbort[thread_id]) {
            Scheduler.Abort[thread_id] += System.nanoTime() - Scheduler.AbortStart[thread_id];
            Scheduler.isAbort[thread_id] = false;
        }
    }

    public static void COMPUTE_SCHEDULE_REDO_COUNT(int thread_id) {
        Scheduler.RedoCount[thread_id]++;
    }

    public static void COMPUTE_CONSTRUCT_START(int thread_id) {
        Scheduler.ConstructStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_CONSTRUCT(int thread_id) {
        Scheduler.Construct[thread_id] += System.nanoTime() - Scheduler.ConstructStart[thread_id];
    }

    public static void COMPUTE_SWITCH_START(int thread_id) {
        Scheduler.SchedulerSwitchStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_SWITCH(int thread_id) {
        Scheduler.SchedulerSwitch[thread_id] = System.nanoTime() - Scheduler.SchedulerSwitchStart[thread_id];
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

    public static void COMPUTE_RECOVERY_ABORT_PUSHDOWN_START(int thread_id) {
        RecoveryPerformance.AbortPushDownStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_RECOVERY_ABORT_PUSHDOWN(int thread_id) {
        RecoveryPerformance.AbortPushDown[thread_id] += System.nanoTime() - RecoveryPerformance.AbortPushDownStart[thread_id];
    }

    public static void COMPUTE_RECOVERY_HISTORY_INSPECTION_START(int thread_id) {
        RecoveryPerformance.HistoryInspectionStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_RECOVERY_HISTORY_INSPECTION(int thread_id) {
        RecoveryPerformance.HistoryInspection[thread_id] += System.nanoTime() - RecoveryPerformance.HistoryInspectionStart[thread_id];
    }

    public static void COMPUTE_RECOVERY_TASK_PLACING_START(int thread_id) {
        RecoveryPerformance.TaskPlacingStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_RECOVERY_TASK_PLACING(int thread_id) {
        RecoveryPerformance.TaskPlacing[thread_id] += System.nanoTime() - RecoveryPerformance.TaskPlacingStart[thread_id];
    }

    public static void COMPUTE_RECOVERY_CONSTRUCT_GRAPH_START(int thread_id) {
        RecoveryPerformance.ConstructGraphStart[thread_id] = System.nanoTime();
    }

    public static void COMPUTE_RECOVERY_CONSTRUCT_GRAPH(int thread_id) {
        RecoveryPerformance.ConstructGraph[thread_id] += System.nanoTime() - RecoveryPerformance.ConstructGraphStart[thread_id];
    }

    public static void COMPUTE_THROUGHPUT(int thread_id, long count, double interval) {
        double throughput = (count - RuntimePerformance.count[thread_id]) / interval;// k tuples per second
        RuntimePerformance.count[thread_id] = count;
        RuntimePerformance.Throughput[thread_id].addValue(throughput);
    }

    public static void COMPUTE_LATENCY(int thread_id, double latency) {
        RuntimePerformance.Latency[thread_id].addValue(latency);
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

        public static void Initialize(int i) {
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

    static class Runtime {
        public static long[] Start = new long[kMaxThreadNum];
        public static long[] PrepareStart = new long[kMaxThreadNum];
        public static long[] Prepare = new long[kMaxThreadNum];
        public static long[] PostStart = new long[kMaxThreadNum];
        public static long[] Post = new long[kMaxThreadNum];
        public static long[] TxnStart = new long[kMaxThreadNum];
        public static long[] Txn = new long[kMaxThreadNum];
        //Runtime throughput per phase
        public static HashMap<Integer, List<Double>> ThroughputPerPhase = new HashMap<>();
        //USED ONLY BY TStream
        public static long[] PreTxnStart = new long[kMaxThreadNum];
        public static long[] PreTxn = new long[kMaxThreadNum];
        //USED ONLY BY FaultTolerant
        public static long[] CompressionStart = new long[kMaxThreadNum];
        public static long[] Compression = new long[kMaxThreadNum];
        public static long[] PersistStart = new long[kMaxThreadNum];
        public static long[] Persist = new long[kMaxThreadNum];
        public static long[] SnapshotStart = new long[kMaxThreadNum];
        public static long[] Snapshot = new long[kMaxThreadNum];
        public static long[] LoggingStart = new long[kMaxThreadNum];
        public static long[] Logging = new long[kMaxThreadNum];

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
                CompressionStart[i] = 0;
                Compression[i] = 0;
                PersistStart[i] = 0;
                Persist[i] = 0;
                SnapshotStart[i] = 0;
                Snapshot[i] = 0;
                LoggingStart[i] = 0;
                Logging[i] = 0;
                ThroughputPerPhase.put(i, new ArrayList<>());
            }
        }
    }

    static class Total_Record {
        public static DescriptiveStatistics[] totalProcessTimePerEvent = new DescriptiveStatistics[kMaxThreadNum];//total time spend for every input event.
        public static DescriptiveStatistics[] compression_total = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] persist_total = new DescriptiveStatistics[kMaxThreadNum];//total time spend in persist.
        public static DescriptiveStatistics[] txn_total = new DescriptiveStatistics[kMaxThreadNum];//total time spend in txn.
        public static DescriptiveStatistics[] stream_total = new DescriptiveStatistics[kMaxThreadNum];//total time spend in stream processing.
        public static DescriptiveStatistics[] overhead_total = new DescriptiveStatistics[kMaxThreadNum];//other overheads.
        public static DescriptiveStatistics[] serialization_total = new DescriptiveStatistics[kMaxThreadNum];//serialization time.
        public static DescriptiveStatistics[] snapshot_serialization_total = new DescriptiveStatistics[kMaxThreadNum];//snapshot_serialization time.

        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                totalProcessTimePerEvent[i] = new DescriptiveStatistics();
                compression_total[i] = new DescriptiveStatistics();
                persist_total[i] = new DescriptiveStatistics();
                txn_total[i] = new DescriptiveStatistics();
                stream_total[i] = new DescriptiveStatistics();
                overhead_total[i] = new DescriptiveStatistics();
                serialization_total[i] = new DescriptiveStatistics();
                snapshot_serialization_total[i] = new DescriptiveStatistics();
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
        public static int[] UsefulCount = new int[kMaxThreadNum];//Used to measure abort for non-structured scheduler(OPNSA, OGNSA)
        public static long[] AbortStart = new long[kMaxThreadNum];
        public static long[] Abort = new long[kMaxThreadNum];
        public static int[] RedoCount = new int[kMaxThreadNum];//Used to measure abort for non-structured scheduler(OPNSA, OGNSA)
        public static long[] Explore = new long[kMaxThreadNum];
        public static long[] ExploreStart = new long[kMaxThreadNum];
        public static long[] ConstructStart = new long[kMaxThreadNum];
        public static long[] Construct = new long[kMaxThreadNum];
        public static long[] TrackingStart = new long[kMaxThreadNum];
        public static long[] Tracking = new long[kMaxThreadNum];
        public static long[] WaitStart = new long[kMaxThreadNum];
        public static long[] Wait = new long[kMaxThreadNum];
        //Not used
        public static long[] NotifyStart = new long[kMaxThreadNum];
        public static long[] Notify = new long[kMaxThreadNum];
        public static long[] FirstExploreStart = new long[kMaxThreadNum];
        public static long[] FirstExplore = new long[kMaxThreadNum];
        public static long[] CachingStart = new long[kMaxThreadNum];
        public static long[] Caching = new long[kMaxThreadNum];
        public static long[] SchedulerSwitchStart = new long[kMaxThreadNum];
        public static long[] SchedulerSwitch = new long[kMaxThreadNum];
        public static boolean[] isAbort = new boolean[kMaxThreadNum];


        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                NextStart[i] = 0;
                Next[i] = 0;
                UsefulStart[i] = 0;
                Useful[i] = 0;
                AbortStart[i] = 0;
                Abort[i] = 0;
                ExploreStart[i] = 0;
                Explore[i] = 0;
                ConstructStart[i] = 0;
                Construct[i] = 0;
                TrackingStart[i] = 0;
                Tracking[i] = 0;
                WaitStart[i] = 0;
                Wait[i] = 0;
                NotifyStart[i] = 0;
                Notify[i] = 0;
                FirstExploreStart[i] = 0;
                FirstExplore[i] = 0;
                CachingStart[i] = 0;
                Caching[i] = 0;
                SchedulerSwitchStart[i] = 0;
                SchedulerSwitch[i] = 0;
                isAbort[i] = false;
                UsefulCount[i] = 0;
                RedoCount[i] = 0;
            }
        }

        public static void Initialize(int i) {
            NextStart[i] = 0;
            Next[i] = 0;
            UsefulStart[i] = 0;
            Useful[i] = 0;
            AbortStart[i] = 0;
            Abort[i] = 0;
            ExploreStart[i] = 0;
            Explore[i] = 0;
            ConstructStart[i] = 0;
            Construct[i] = 0;
            TrackingStart[i] = 0;
            Tracking[i] = 0;
            WaitStart[i] = 0;
            Wait[i] = 0;
            NotifyStart[i] = 0;
            Notify[i] = 0;
            FirstExploreStart[i] = 0;
            FirstExplore[i] = 0;
            CachingStart[i] = 0;
            Caching[i] = 0;
            SchedulerSwitchStart[i] = 0;
            SchedulerSwitch[i] = 0;
            isAbort[i] = false;
            UsefulCount[i] = 0;
            RedoCount[i] = 0;
        }
    }

    static class Scheduler_Record {
        public static DescriptiveStatistics[] Explore = new DescriptiveStatistics[kMaxThreadNum];//Explore time.
        public static DescriptiveStatistics[] Useful = new DescriptiveStatistics[kMaxThreadNum];//Useful_work time.
        public static DescriptiveStatistics[] Abort = new DescriptiveStatistics[kMaxThreadNum];//Abort time.
        public static DescriptiveStatistics[] Construct = new DescriptiveStatistics[kMaxThreadNum];//Construction time.
        public static DescriptiveStatistics[] Tracking = new DescriptiveStatistics[kMaxThreadNum];//Tracking time.
        public static DescriptiveStatistics[] Wait = new DescriptiveStatistics[kMaxThreadNum];//Wait time.
        public static DescriptiveStatistics[] Notify = new DescriptiveStatistics[kMaxThreadNum];//Notify time.
        public static DescriptiveStatistics[] Next = new DescriptiveStatistics[kMaxThreadNum];//Next time.

        public static DescriptiveStatistics[] FirstExplore = new DescriptiveStatistics[kMaxThreadNum];//First explore time.
        public static DescriptiveStatistics[] Caching = new DescriptiveStatistics[kMaxThreadNum];//Caching time.
        public static DescriptiveStatistics[] SchedulerSwitch = new DescriptiveStatistics[kMaxThreadNum];//Scheduler-switch time.


        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                Next[i] = new DescriptiveStatistics();
                Explore[i] = new DescriptiveStatistics();
                Useful[i] = new DescriptiveStatistics();
                Abort[i] = new DescriptiveStatistics();
                Construct[i] = new DescriptiveStatistics();
                Tracking[i] = new DescriptiveStatistics();
                Wait[i] = new DescriptiveStatistics();
                Notify[i] = new DescriptiveStatistics();
                FirstExplore[i] = new DescriptiveStatistics();
                Caching[i] = new DescriptiveStatistics();
                SchedulerSwitch[i] = new DescriptiveStatistics();
            }
        }
    }

    public static class RuntimeHardware extends TimerTask {
        int gb = 1024 * 1024 * 1024;
        int mb = 1024 * 1024;
        int kb = 1024;
        long pFileSize = 0L;
        String rootFilePath;

        public RuntimeHardware(String rootFilePath) {
            this.rootFilePath = rootFilePath;
        }

        @Override
        public void run() {
            long UsedMemory = (java.lang.Runtime.getRuntime().totalMemory() - java.lang.Runtime.getRuntime().freeMemory()) / gb;
            usedMemory.addValue(UsedMemory);
            long FileSize = 0;
            try {
                FileSize = FileSystem.getFileSize(new File(rootFilePath));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (pFileSize == 0L) {
                pFileSize = FileSize;
            } else {
                usedFileSize.addValue((FileSize - pFileSize) / kb);
                SSDBandwidth.addValue((FileSize - pFileSize) * 1E3 / (kb * 10));// kb / s
                pFileSize = FileSize;
            }
        }
    }

    public static class RuntimePerformance {
        public static DescriptiveStatistics[] Latency = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] Throughput = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] SnapshotSize = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] LogSize = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] count = new long[kMaxThreadNum];
        public static long[] lastTasks = new long[kMaxThreadNum];

        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                Latency[i] = new DescriptiveStatistics();
                Throughput[i] = new DescriptiveStatistics();
                SnapshotSize[i] = new DescriptiveStatistics();
                LogSize[i] = new DescriptiveStatistics();
                count[i] = 0;
                lastTasks[i] = -1;
            }
        }
    }

    public static class RecoveryPerformance {
        public static long[] recoveryItems = new long[kMaxThreadNum];
        public static boolean[] stopRecovery = new boolean[kMaxThreadNum];
        public static long[] startRecoveryTime = new long[kMaxThreadNum];
        public static long[] startReloadDatabaseTime = new long[kMaxThreadNum];
        public static long[] startRedoWriteAheadLogTime = new long[kMaxThreadNum];//For pathLogging it is the time used to construct the path.
        public static long[] startReloadInputTime = new long[kMaxThreadNum];
        public static long[] startReplayTime = new long[kMaxThreadNum];
        //in ms.
        public static DescriptiveStatistics[] RecoveryTime = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] ReloadDatabaseTime = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] RedoWriteAheadLogTime = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] ReloadInputTime = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] ReplayTime = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] txn_total = new long[kMaxThreadNum];
        public static long[] stream_total = new long[kMaxThreadNum];
        public static long[] total_time = new long[kMaxThreadNum];
        public static long[] overhead_total = new long[kMaxThreadNum];
        //in ns.
        public static long[] Explore = new long[kMaxThreadNum];//Explore.
        public static long[] Useful = new long[kMaxThreadNum];//Useful_work.
        public static long[] Abort = new long[kMaxThreadNum];//Abort.
        public static long[] Construct = new long[kMaxThreadNum];//Construction.
        public static long[] Wait = new long[kMaxThreadNum];//Wait.
        public static long[] Notify = new long[kMaxThreadNum];//Notify.
        public static long[] Next = new long[kMaxThreadNum];//Next.
        public static long[] FirstExplore = new long[kMaxThreadNum];//First explore.
        public static long[] Caching = new long[kMaxThreadNum];//Caching.
        public static long[] SchedulerSwitch = new long[kMaxThreadNum];//Scheduler switch.

        public static long[] AbortPushDownStart = new long[kMaxThreadNum];
        public static long[] AbortPushDown = new long[kMaxThreadNum];
        public static long[] HistoryInspectionStart = new long[kMaxThreadNum];
        public static long[] HistoryInspection = new long[kMaxThreadNum];
        public static long[] TaskPlacingStart = new long[kMaxThreadNum];
        public static long[] TaskPlacing = new long[kMaxThreadNum];
        //Only used for dependency logging
        public static long[] ConstructGraphStart = new long[kMaxThreadNum];
        public static long[] ConstructGraph = new long[kMaxThreadNum];

        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                stopRecovery[i] = true;
                startRecoveryTime[i] = 0;
                startReloadDatabaseTime[i] = 0;
                startRedoWriteAheadLogTime[i] = 0;
                startReloadInputTime[i] = 0;
                startReplayTime[i] = 0;
                RecoveryTime[i] = new DescriptiveStatistics();
                ReloadDatabaseTime[i] = new DescriptiveStatistics();
                RedoWriteAheadLogTime[i] = new DescriptiveStatistics();
                ReloadInputTime[i] = new DescriptiveStatistics();
                ReplayTime[i] = new DescriptiveStatistics();
                txn_total[i] = 0;
                stream_total[i] = 0;
                total_time[i] = 0;
                overhead_total[i] = 0;
                Next[i] = 0;
                Explore[i] = 0;
                Useful[i] = 0;
                Abort[i] = 0;
                Construct[i] = 0;
                Wait[i] = 0;
                Notify[i] = 0;
                FirstExplore[i] = 0;
                Caching[i] = 0;
                SchedulerSwitch[i] = 0;
                recoveryItems[i] = 0;
                AbortPushDownStart[i] = 0;
                AbortPushDown[i] = 0;
                HistoryInspectionStart[i] = 0;
                HistoryInspection[i] = 0;
                TaskPlacingStart[i] = 0;
                TaskPlacing[i] = 0;
                ConstructGraphStart[i] = 0;
                ConstructGraph[i] = 0;
            }
        }

        public static void COMPUTE_RECOVERY_START(int thread_id) {
            startRecoveryTime[thread_id] = System.nanoTime();
            stopRecovery[thread_id] = false;
        }

        public static void COMPUTE_RECOVERY(int thread_id) {
            RecoveryTime[thread_id].addValue((System.nanoTime() - startRecoveryTime[thread_id]) / 1E6);
        }

        public static void COMPUTE_RELOAD_DATABASE_START(int thread_id) {
            startReloadDatabaseTime[thread_id] = System.nanoTime();
        }

        public static void COMPUTE_RELOAD_DATABASE(int thread_id) {
            ReloadDatabaseTime[thread_id].addValue((System.nanoTime() - startReloadDatabaseTime[thread_id]) / 1E6);
        }

        public static void COMPUTE_REDO_START(int thread_id) {
            startRedoWriteAheadLogTime[thread_id] = System.nanoTime();
        }

        public static void COMPUTE_REDO(int thread_id) {
            RedoWriteAheadLogTime[thread_id].addValue((System.nanoTime() - startRedoWriteAheadLogTime[thread_id]) / 1E6);
        }

        public static void COMPUTE_RELOAD_INPUT_START(int thread_id) {
            startReloadInputTime[thread_id] = System.nanoTime();
        }

        public static void COMPUTE_RELOAD_INPUT(int thread_id) {
            ReloadInputTime[thread_id].addValue((System.nanoTime() - startReloadInputTime[thread_id]) / 1E6);
        }

        public static void COMPUTE_REPLAY_START(int thread_id) {
            startReplayTime[thread_id] = System.nanoTime();
        }

        public static void COMPUTE_REPLAY(int thread_id) {
            ReplayTime[thread_id].addValue((System.nanoTime() - startReplayTime[thread_id]) / 1E6);
        }
    }
}