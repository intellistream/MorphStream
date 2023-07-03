package profiler;

import common.CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.*;
import static common.IRunner.CCOption_MorphStream;
import static java.nio.file.StandardOpenOption.APPEND;
import static profiler.Metrics.*;

public class MeasureTools {
    private static final Logger log = LoggerFactory.getLogger(MeasureTools.class);
    public static AtomicInteger counter = new AtomicInteger(0);

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

    public static void BEGIN_TOTAL_TIME_MEASURE_TS(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            if (Metrics.Runtime.Start[thread_id] == 0)
                COMPUTE_START_TIME(thread_id);
            COMPUTE_PRE_EXE_START_TIME(thread_id);
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

    public static void END_LOCK_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_LOCK_TIME_ACC(thread_id);
    }

    public static void END_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_WAIT_TIME(thread_id);
    }

    public static void END_WAIT_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_WAIT_TIME_ACC(thread_id);
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

    public static void END_TXN_TIME_MEASURE(int thread_id, int number_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_TXN_TIME(thread_id);
            RECORD_TXN_BREAKDOWN_RATIO(thread_id, number_events);
        }
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

    // OGScheduler Specific.
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
        if (enable_debug) counter.incrementAndGet();
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
    public static void BEGIN_SCHEDULER_SWITCH_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SWITCH_START(thread_id);
    }

    public static void END_SCHEDULER_SWITCH_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SWITCH(thread_id);
    }

    public static void BEGIN_CACHE_OPERATION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_CACHE_OPERATION_START(thread_id);
    }

    public static void END_CACHE_OPERATION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_CACHE_OPERATION(thread_id);
    }

    public static void BEGIN_FIRST_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_FIRST_EXPLORE_START(thread_id);
    }

    public static void END_FIRST_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_FIRST_EXPLORE(thread_id);
    }

    public static void BEGIN_NOTIFY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_NOTIFY_START(thread_id);
    }

    public static void END_NOTIFY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_NOTIFY(thread_id);
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
                for (int i = 0; i < Total_Record.totalProcessTimePerEvent[threadId].getValues().length; i++) {
                    output = String.format("%d\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f"
                            , i
                            , Total_Record.totalProcessTimePerEvent[threadId].getValues()[i]
                            , Total_Record.stream_total[threadId].getValues()[i]
                            , Total_Record.txn_total[threadId].getValues()[i]
                            , Total_Record.overhead_total[threadId].getValues()[i]
                    );
                    log.info(output);
                }
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
                        , ccOption == CCOption_MorphStream ? 0 : Transaction_Record.lock_ratio[threadId].getMean()
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
            if (enable_debug) log.info("++++++ counter: " + counter);
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("SchedulerTimeBreakdownReport\n");
            if (enable_log) log.info("===OGScheduler Time Breakdown Report===");
            fileWriter.write("thread_id\t explore_time\t next_time\t useful_time\t notify_time\t construct_time\t first_explore_time\t scheduler_switch\n");
            if (enable_log)
                log.info("thread_id\t explore_time\t next_time\t useful_time\t notify_time\t construct_time\t first_explore_time\t scheduler_switch");
            for (int threadId = 0; threadId < tthread; threadId++) {
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , Scheduler_Record.Explore[threadId].getMean()
                        , Scheduler_Record.Next[threadId].getMean()
                        , Scheduler_Record.Useful[threadId].getMean()
                        , Scheduler_Record.Noitfy[threadId].getMean()
                        , Scheduler_Record.Construct[threadId].getMean()
                        , Scheduler_Record.FirstExplore[threadId].getMean()
                        , Scheduler_Record.SchedulerSwitch[threadId].getMean()
                );
                fileWriter.write(output + "\n");
                if (enable_log) log.info(output);
                for (int i = 0; i < Scheduler_Record.Construct[threadId].getValues().length; i++) {
                    output = String.format("%d\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f\t" +
                                    "%-10.2f\t"
                            , i
                            , Scheduler_Record.Explore[threadId].getValues()[i]
                            , Scheduler_Record.Next[threadId].getValues()[i]
                            , Scheduler_Record.Useful[threadId].getValues()[i]
                            , Scheduler_Record.Noitfy[threadId].getValues()[i]
                            , Scheduler_Record.Construct[threadId].getValues()[i]
                            , Scheduler_Record.FirstExplore[threadId].getValues()[i]
                            , Scheduler_Record.SchedulerSwitch[threadId].getValues()[i]
                    );
                    log.info(output);
                }
            }
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void WriteMemoryConsumption(File file) {
        if (enable_memory_measurement) {
            timer.cancel();
            try {
                FileWriter f = null;
                f = new FileWriter(new File(file.getPath() + ".txt"));
                Writer w = new BufferedWriter(f);
                w.write("UsedMemory\n");
                for (int i = 0; i < usedMemory.getValues().length; i ++){
                    String output = String.format("%f\t" +
                                    "%-10.4f\t"
                            , (float) i ,usedMemory.getValues()[i]);
                    w.write(output + "\n");
                }
                w.close();
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void WriteThroughputReport(File file, double throughput) {
        try {
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("Throughput: " + throughput + "\n");
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void WriteThroughputReportRuntime(File file, int tthread, int phase, int shiftRate) {
        try {
            double[] tr = new double[Metrics.Runtime.ThroughputPerPhase.get(0).size()];
            //every punctuation
            for (int i = 0; i < tr.length; i++) {
                tr[i] = 0;
                for (int j = 0; j < tthread; j++){
                    tr[i] = tr[i] + Metrics.Runtime.ThroughputPerPhase.get(j).get(i) * 1E6;//
                }
            }
            //every phase
            double[] tr_p = new double[tr.length / shiftRate];
            for (int i = 0; i < tr_p.length; i++) {
                tr_p[i] = 0;
                for (int j = i * shiftRate; j < (i + 1) * shiftRate; j++) {
                    tr_p[i] = tr_p[i] + tr[j];
                }
                tr_p[i] = tr_p[i] / shiftRate;
            }
            StringBuilder stringBuilder = new StringBuilder();
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("phase_id\t throughput\n");
            for (int i = 0; i < tr_p.length; i++){
                String output = String.format("%d\t" +
                                "%-10.4f\t"
                        , i,tr_p[i]
                );
                stringBuilder.append(output);
                fileWriter.write(output + "\n");
            }
            fileWriter.close();
            if (enable_log) log.info(String.valueOf(stringBuilder));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void METRICS_REPORT(int ccOption, File file, int tthread, double throughput, int phase, int shiftRate) {
        WriteThroughputReport(file, throughput);
        AverageTotalTimeBreakdownReport(file, tthread);
        WriteThroughputReportRuntime(file, tthread, phase, shiftRate);
        WriteMemoryConsumption(file);
        if (ccOption == CCOption_MorphStream) {//extra info
            SchedulerTimeBreakdownReport(file, tthread);
        } else {
            TransactionBreakdownRatioReport(ccOption, file, tthread);
        }
    }
}
