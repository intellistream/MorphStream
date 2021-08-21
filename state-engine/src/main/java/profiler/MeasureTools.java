package profiler;

import common.CONTROL;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static common.CONTROL.enable_log;
import static common.IRunner.CCOption_TStream;
import static java.nio.file.StandardOpenOption.APPEND;
import static profiler.Metrics.*;

@lombok.extern.slf4j.Slf4j
public class MeasureTools {

    public static int counter = 0;

    public static void Initialize() {
        Metrics.TxnRuntime.Initialize();
        Metrics.Runtime.Initialize();
        Metrics.Scheduler.Initialize();
        Metrics.Total_Record.Initialize();
        Metrics.Transaction_Record.Initialize();
        Metrics.Scheduler_Record.Initialize();
    }

    public static void SCHEDULE_TIME_RECORD(int threadId, int num_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RECORD_SCHEDULE_TIME(threadId, num_events);
    }

    public static void BEGIN_TOTAL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            if (Metrics.Runtime.Start[thread_id] == 0)
                COMPUTE_START_TIME(thread_id);
            COMPUTE_PRE_EXE_START_TIME(thread_id);
        }
    }

    public static void END_TOTAL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            RECORD_TIME(thread_id);
            RESET_COUNTERS(thread_id);
        }
    }

    public static void END_TOTAL_TIME_MEASURE_TS(int thread_id, int number_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            RECORD_TIME(thread_id, number_events);
            RESET_COUNTERS(thread_id);
        }
    }

    public static void END_PREPARE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_PRE_EXE_TIME(thread_id);
        }
    }

    public static void END_PREPARE_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_PRE_EXE_TIME_ACC(thread_id);
        }
    }

    public static void BEGIN_INDEX_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_INDEX_TIME(thread_id);
    }

    public static void END_INDEX_TIME_MEASURE_ACC(int thread_id, boolean is_retry_) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_INDEX_TIME_ACC(thread_id);
        }
    }

    public static void BEGIN_POST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_POST_EXE_TIME(thread_id);
    }

    public static void END_POST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_POST_EXE_TIME(thread_id);
    }

    public static void END_POST_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_POST_EXE_TIME_ACC(thread_id);
    }

    public static void BEGIN_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_WAIT_TIME(thread_id);
    }

    public static void BEGIN_LOCK_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_LOCK_TIME(thread_id);
    }

    public static void END_LOCK_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_LOCK_TIME(thread_id);
    }

    public static void END_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_WAIT_TIME(thread_id);
    }

    public static void BEGIN_ABORT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_ABORT_START_TIME(thread_id);
    }

    public static void END_ABORT_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_ABORT_TIME_ACC(thread_id);
    }

    public static void BEGIN_ACCESS_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_ACCESS_TIME(thread_id);
    }

    //needs to include write compute time also for TS.
    public static void END_ACCESS_TIME_MEASURE_TS(int thread_id, int read_size,
                                                  double write_useful_time, int write_size) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            double write_time = write_useful_time * write_size;
            COMPUTE_ACCESS_TIME(thread_id, read_size, write_time);
        }
    }

    public static void END_ACCESS_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_ACCESS_TIME_ACC(thread_id);
    }

    public static void BEGIN_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_TXN_START_TIME(thread_id);
    }

    public static void END_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_TXN_TIME(thread_id);
            RECORD_TXN_BREAKDOWN_RATIO(thread_id);
        }
    }

    //TStream Specific.
    public static void BEGIN_PRE_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_PRE_TXN_START_TIME(thread_id);
    }

    public static void END_PRE_TXN_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_PRE_TXN_TIME_ACC(thread_id);
    }

    // Scheduler Specific.
    public static void BEGIN_SCHEDULE_NEXT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_NEXT_START(thread_id);
    }

    public static void END_SCHEDULE_NEXT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_NEXT_ACC(thread_id);
    }

    public static void BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_EXPLORE_START(thread_id);
    }

    public static void END_SCHEDULE_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_EXPLORE_ACC(thread_id);
    }

    public static void BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(int thread_id) {
        counter++;
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_USEFUL_START(thread_id);
    }

    public static void END_SCHEDULE_USEFUL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_USEFUL(thread_id);
    }

    public static void BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_CONSTRUCT_START(thread_id);
    }

    public static void END_TPG_CONSTRUCTION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_CONSTRUCT(thread_id);
    }

    private static void AverageTotalTimeBreakdownReport(File file, int tthread) {
        try {
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("AverageTotalTimeBreakdownReport\n");
            if (enable_log) log.info("===Average Total Time Breakdown Report===");
            fileWriter.write("thread_id\t total_time\t stream_process\t txn_process\t overheads\n");
            if (enable_log) log.info("thread_id\t total_time\t stream_process\t txn_process\t overheads");
            for (int threadId = 0; threadId < tthread; threadId++) {
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f"
                        , threadId
                        , Total_Record.totalProcessTimePerEvent[threadId].getMean()
                        , Total_Record.stream_total[threadId].getMean()
                        , Total_Record.txn_total[threadId].getMean()
                        , Total_Record.overhead_total[threadId].getMean()
                );
                fileWriter.write(output + "\n");
                if (enable_log) log.info(output);
            }
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void TransactionBreakdownRatioReport(int ccOption, File file, int tthread) {
        try {
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("TransactionBreakdownRatioReport\n");
            if (enable_log) log.info("===TXN BREAKDOWN===");
            fileWriter.write("thread_id\t index_ratio\t useful_ratio\t sync_ratio\t lock_ratio\n");
            if (enable_log) log.info("thread_id\t index_ratio\t useful_ratio\t sync_ratio\t lock_ratio");
            for (int threadId = 0; threadId < tthread; threadId++) {
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , Transaction_Record.index_ratio[threadId].getMean()
                        , Transaction_Record.useful_ratio[threadId].getMean()
                        , Transaction_Record.sync_ratio[threadId].getMean()
                        , ccOption == CCOption_TStream ? 0 : Transaction_Record.lock_ratio[threadId].getMean()
                );
                fileWriter.write(output + "\n");
                if (enable_log) log.info(output);
            }
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void SchedulerTimeBreakdownReport(File file, int tthread) {
        try {
            System.out.println("++++++ counter: " + counter);
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("SchedulerTimeBreakdownReport\n");
            if (enable_log) log.info("===Scheduler Time Breakdown Report===");
            fileWriter.write("thread_id\t explore_time\t next_time\t useful_time\t construct_time\n");
            if (enable_log) log.info("thread_id\t explore_time\t next_time\t useful_time\t construct_time");
            for (int threadId = 0; threadId < tthread; threadId++) {
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , Scheduler_Record.Explore[threadId].getMean()
                        , Scheduler_Record.Next[threadId].getMean()
                        , Scheduler_Record.Useful[threadId].getMean()
                        , Scheduler_Record.Construct[threadId].getMean()
                );
                fileWriter.write(output + "\n");
                if (enable_log) log.info(output);
            }
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void METRICS_REPORT(int ccOption, File file, int tthread) {
        AverageTotalTimeBreakdownReport(file, tthread);
        if (ccOption == CCOption_TStream) {//extra info
            SchedulerTimeBreakdownReport(file, tthread);
        } else {
            TransactionBreakdownRatioReport(ccOption, file, tthread);
        }
    }
}
