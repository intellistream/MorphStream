package intellistream.morphstream.engine.txn.profiler;

import com.fasterxml.jackson.databind.ObjectMapper;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.configuration.Constants.CCOption_MorphStream;
import static intellistream.morphstream.util.FaultToleranceConstants.*;
import static java.nio.file.StandardOpenOption.*;

public class MeasureTools {
    private static final Logger log = LoggerFactory.getLogger(MeasureTools.class);
    public static AtomicInteger counter = new AtomicInteger(0);
    private static LocalDateTime jobStartTime;
    private static final Random jobIdGenerator = new Random();  // used to generate random ids for running jobs

    public static void Initialize() {
        Metrics.TxnRuntime.Initialize();
        Metrics.Runtime.Initialize();
        Metrics.Scheduler.Initialize();
        Metrics.Total_Record.Initialize();
        Metrics.Transaction_Record.Initialize();
        Metrics.Scheduler_Record.Initialize();
        Metrics.RuntimePerformance.Initialize();
        Metrics.RecoveryPerformance.Initialize();
        jobStartTime = LocalDateTime.now();
        RuntimeMonitor.Initialize();
    }

    public static void SCHEDULE_TIME_RECORD(int threadId, int num_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RECORD_SCHEDULE_TIME(threadId, num_events);
    }

    public static void BEGIN_TOTAL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            Metrics.COMPUTE_START_TIME(thread_id);
            Metrics.COMPUTE_PRE_EXE_START_TIME(thread_id);
        }
    }

    public static void END_TOTAL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            Metrics.RECORD_TIME(thread_id);
            Metrics.RESET_COUNTERS(thread_id);
        }
    }

    public static void BEGIN_TOTAL_TIME_MEASURE_TS(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            if (Metrics.Runtime.Start[thread_id] == 0)
                Metrics.COMPUTE_START_TIME(thread_id);
            Metrics.COMPUTE_PRE_EXE_START_TIME(thread_id);
        }
    }

    public static void END_TOTAL_TIME_MEASURE_TS(int thread_id, int number_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            Metrics.RECORD_TIME(thread_id, number_events);
            Metrics.RESET_COUNTERS(thread_id);
        }
    }


    public static void END_PREPARE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            Metrics.COMPUTE_PRE_EXE_TIME(thread_id);
        }
    }

    public static void END_PREPARE_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            Metrics.COMPUTE_PRE_EXE_TIME_ACC(thread_id);
        }
    }

    public static void BEGIN_INDEX_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_START_INDEX_TIME(thread_id);
    }

    public static void END_INDEX_TIME_MEASURE_ACC(int thread_id, boolean is_retry_) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            Metrics.COMPUTE_INDEX_TIME_ACC(thread_id);
        }
    }

    public static void BEGIN_POST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_START_POST_EXE_TIME(thread_id);
    }

    public static void END_POST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_POST_EXE_TIME(thread_id);
    }

    public static void END_POST_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_POST_EXE_TIME_ACC(thread_id);
    }

    public static void BEGIN_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_START_WAIT_TIME(thread_id);
    }

    public static void BEGIN_LOCK_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_START_LOCK_TIME(thread_id);
    }

    public static void END_LOCK_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_LOCK_TIME(thread_id);
    }

    public static void END_LOCK_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_LOCK_TIME_ACC(thread_id);
    }

    public static void END_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_WAIT_TIME(thread_id);
    }

    public static void END_WAIT_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_WAIT_TIME_ACC(thread_id);
    }

    public static void BEGIN_ABORT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_ABORT_START_TIME(thread_id);
    }

    public static void END_ABORT_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_ABORT_TIME_ACC(thread_id);
    }

    public static void BEGIN_ACCESS_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_START_ACCESS_TIME(thread_id);
    }

    //needs to include write compute time also for TS.
    public static void END_ACCESS_TIME_MEASURE_TS(int thread_id, int read_size,
                                                  double write_useful_time, int write_size) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            double write_time = write_useful_time * write_size;
            Metrics.COMPUTE_ACCESS_TIME(thread_id, read_size, write_time);
        }
    }

    public static void END_ACCESS_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_ACCESS_TIME_ACC(thread_id);
    }

    public static void BEGIN_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_TXN_START_TIME(thread_id);
    }

    public static void END_TXN_TIME_MEASURE(int thread_id, int number_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            Metrics.COMPUTE_TXN_TIME(thread_id);
            Metrics.RECORD_TXN_BREAKDOWN_RATIO(thread_id, number_events);
        }
    }

    public static void END_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            Metrics.COMPUTE_TXN_TIME(thread_id);
//            Metrics.RECORD_TXN_BREAKDOWN_RATIO(thread_id); //TODO: Support breakdown measure
        }
    }

    //TStream Specific.
    public static void BEGIN_PRE_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_PRE_TXN_START_TIME(thread_id);
    }

    public static void END_PRE_TXN_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_PRE_TXN_TIME_ACC(thread_id);
    }

    //Fault Tolerance Specific.
    public static void BEGIN_COMPRESSION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_COMPRESSION_START_TIME(thread_id);
    }

    public static void END_COMPRESSION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_COMPRESSION_TIME(thread_id);
    }

    public static void BEGIN_PERSIST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_PERSIST_START_TIME(thread_id);
    }

    public static void END_PERSIST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_PERSIST_TIME(thread_id);
    }

    public static void BEGIN_SNAPSHOT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SNAPSHOT_START_TIME(thread_id);
    }

    public static void END_SNAPSHOT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SNAPSHOT_TIME(thread_id);
    }

    public static void BEGIN_LOGGING_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_LOGGING_START_TIME(thread_id);
    }

    public static void END_LOGGING_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_LOGGING_TIME(thread_id);
    }

    // OGScheduler Specific.
    public static void BEGIN_SCHEDULE_NEXT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_NEXT_START(thread_id);
    }

    public static void END_SCHEDULE_NEXT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_NEXT_ACC(thread_id);
    }

    public static void BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_EXPLORE_START(thread_id);
    }

    public static void END_SCHEDULE_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_EXPLORE_ACC(thread_id);
    }

    public static void BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(int thread_id) {
        if (enable_debug) counter.incrementAndGet();
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted() && !Metrics.Scheduler.isAbort[thread_id])
            Metrics.COMPUTE_SCHEDULE_USEFUL_START(thread_id);
    }

    public static void END_SCHEDULE_USEFUL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted() && !Metrics.Scheduler.isAbort[thread_id])
            Metrics.COMPUTE_SCHEDULE_USEFUL(thread_id);
    }

    public static void BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_TRACKING_START(thread_id);
    }

    public static void END_SCHEDULE_TRACKING_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_TRACKING(thread_id);
    }

    public static void BEGIN_SCHEDULE_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_WAIT_START(thread_id);
    }

    public static void END_SCHEDULE_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_WAIT(thread_id);
    }

    public static void BEGIN_RECOVERY_CONSTRUCT_GRAPH_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_RECOVERY_CONSTRUCT_GRAPH_START(thread_id);
    }

    public static void END_RECOVERY_CONSTRUCT_GRAPH_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_RECOVERY_CONSTRUCT_GRAPH(thread_id);
    }

    public static void BEGIN_SCHEDULE_ABORT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_ABORT_START(thread_id);
    }

    public static void END_SCHEDULE_ABORT_TIME_MEASURE(int thread_id) {
        if (enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_ABORT(thread_id);
    }

    public static void SCHEDULE_REDO_COUNT_MEASURE(int thread_id) {
        if (enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SCHEDULE_REDO_COUNT(thread_id);
    }

    public static void BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_CONSTRUCT_START(thread_id);
    }

    public static void END_TPG_CONSTRUCTION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_CONSTRUCT(thread_id);
    }

    public static void BEGIN_SCHEDULER_SWITCH_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SWITCH_START(thread_id);
    }

    public static void END_SCHEDULER_SWITCH_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_SWITCH(thread_id);
    }

    public static void BEGIN_CACHE_OPERATION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_CACHE_OPERATION_START(thread_id);
    }

    public static void END_CACHE_OPERATION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_CACHE_OPERATION(thread_id);
    }

    public static void BEGIN_FIRST_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_FIRST_EXPLORE_START(thread_id);
    }

    public static void END_FIRST_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_FIRST_EXPLORE(thread_id);
    }

    public static void BEGIN_NOTIFY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_NOTIFY_START(thread_id);
    }

    public static void END_NOTIFY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_NOTIFY(thread_id);
    }

    public static void BEGIN_RECOVERY_ABORT_PUSH_DOWN_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_RECOVERY_ABORT_PUSHDOWN_START(thread_id);
    }

    public static void END_RECOVERY_ABORT_PUSH_DOWN_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_RECOVERY_ABORT_PUSHDOWN(thread_id);
    }

    public static void BEGIN_RECOVERY_HISTORY_INSPECT_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_RECOVERY_HISTORY_INSPECTION_START(thread_id);
    }

    public static void END_RECOVERY_HISTORY_INSPECT_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_RECOVERY_HISTORY_INSPECTION(thread_id);
    }

    public static void BEGIN_RECOVERY_TASK_PLACING_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_RECOVERY_TASK_PLACING_START(thread_id);
    }

    public static void END_RECOVERY_TASK_PLACING_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_RECOVERY_TASK_PLACING(thread_id);
    }

    public static void THROUGHPUT_MEASURE(int thread_id, long count, double interval) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_THROUGHPUT(thread_id, count, interval);
    }

    public static void LATENCY_MEASURE(int thread_id, double latency) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.COMPUTE_LATENCY(thread_id, latency);
    }

    public static void setMetricDirectory(String directory) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.directory = directory;
    }

    public static void setMetricFileNameSuffix(String suffix) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.fileNameSuffix = suffix;
    }

    // Fault Tolerance Specific.
    public static void setSnapshotSize(int thread_id, double size) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RuntimePerformance.SnapshotSize[thread_id].addValue(size);
    }

    public static void setLogSize(int thread_id, double size) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RuntimePerformance.LogSize[thread_id].addValue(size);
    }

    // Recovery Time Specific.
    public static void BEGIN_RECOVERY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_RECOVERY_START(thread_id);
    }

    public static void END_RECOVERY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_RECOVERY(thread_id);
    }

    public static void BEGIN_RELOAD_DATABASE_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_RELOAD_DATABASE_START(thread_id);
    }

    public static void END_RELOAD_DATABASE_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_RELOAD_DATABASE(thread_id);
    }

    public static void BEGIN_REDO_WAL_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_REDO_START(thread_id);
    }

    public static void END_REDO_WAL_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_REDO(thread_id);
    }

    public static void BEGIN_RELOAD_INPUT_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_RELOAD_INPUT_START(thread_id);
    }

    public static void END_RELOAD_INPUT_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_RELOAD_INPUT(thread_id);
    }

    public static void BEGIN_REPLAY_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_REPLAY_START(thread_id);
    }

    public static void END_REPLAY_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.RecoveryPerformance.COMPUTE_REPLAY(thread_id);
    }

    private static void WriteThroughputReport(double throughput) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            file.mkdirs();
            if (file.exists()) {
                try {
                    file.delete();
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("Throughput: " + throughput + "\n");
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void WriteJSONData(Configuration config, int tthread, double totalThroughput, int snapshotInterval) {
        // TODO: Implement Job Data class
//        Job job = new Job();
//        job.setAppId(jobIdGenerator.nextInt());
//        job.setNthreads(tthread);
//        job.setName(config.getString("application", "app"));
//        job.setNEvents(config.getInt("totalEvents", 0));
//        job.setThroughput(totalThroughput);
//        job.setIsRunning(false);
//
//        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//        job.setStartTime(jobStartTime.format(dateTimeFormatter));
//
//        LocalDateTime currentTime = LocalDateTime.now();
//
//        Duration duration = Duration.between(jobStartTime, currentTime);
//
//        long hours = duration.toHours();
//        long minutes = duration.toMinutes() - hours * 60;
//        long seconds = duration.getSeconds() - hours * 60 * 60 - minutes * 60;
//
//        Formatter formatter = new Formatter();
//        formatter.format("%02d:%02d:%02d", hours, minutes, seconds);
//        job.setDuration(formatter.toString());
//
//        double totalProcessTime = 0;
//        double totalSerializeTime = 0;
//        double totalPersistTime = 0;
//        double totalStreamProcessTime = 0;
//        double totalTxnProcessTime = 0;
//        double totalOverheads = 0;
//        for (int threadId = 0; threadId < tthread; threadId++) {
//            totalProcessTime += Metrics.Total_Record.totalProcessTimePerEvent[threadId].getMean();
//            if (snapshotInterval != 0) {
//                totalSerializeTime += Metrics.Total_Record.compression_total[threadId].getMean() + Total_Record.serialization_total[threadId].getMean() + Total_Record.snapshot_serialization_total[threadId].getMean() / snapshotInterval;
//            }
//            totalPersistTime += Metrics.Total_Record.persist_total[threadId].getMean();
//            totalStreamProcessTime += Metrics.Total_Record.stream_total[threadId].getMean();
//            totalTxnProcessTime += Metrics.Total_Record.txn_total[threadId].getMean();
//            totalOverheads += Metrics.Total_Record.overhead_total[threadId].getMean();
//        }
//
//        // nanosecond to millisecond
//        job.setTotalTimeBreakdown(new TotalTimeBreakdown(totalProcessTime/1E6, totalSerializeTime/1E6, totalPersistTime/1E6, totalStreamProcessTime/1E6, totalOverheads/1E6, totalTxnProcessTime/1E6));
//
//        double explore_time = 0;
//        double useful_time = 0;
//        double abort_time = 0;
//        double construct_time = 0;
//        double tracking_time = 0;
//        for (int threadId = 0; threadId < tthread; threadId++) {
//            explore_time += Metrics.Scheduler_Record.Explore[threadId].getMean();
//            useful_time += Metrics.Scheduler_Record.Useful[threadId].getMean();
//            abort_time += Metrics.Scheduler_Record.Abort[threadId].getMean();
//            construct_time += Metrics.Scheduler_Record.Construct[threadId].getMean();
//            tracking_time += Metrics.Scheduler_Record.Tracking[threadId].getMean();
//        }
//
//        job.setSchedulerTimeBreakdown(new SchedulerTimeBreakdown(explore_time/1E6, useful_time/1E6, abort_time/1E6, construct_time/1E6, tracking_time/1E6));
//
//        ObjectMapper objectMapper = new ObjectMapper();
//
//        List<double[]> latencys = new ArrayList<>();
//        List<double[]> throughputs = new ArrayList<>();
//        List<Double> periodicalLatency = new ArrayList<>();
//        List<Double> periodicalThroughput = new ArrayList<>();
//
//        for (int i = 0; i < tthread; i++) {
//            latencys.add(Metrics.BatchRuntimeData.Latency[i].getValues());
//            throughputs.add(Metrics.BatchRuntimeData.Throughput[i].getValues());
//        }
//        loop: for (int i = 0; i < latencys.get(0).length; i++) {
//            double throughput = 0;
//            double latency = 0;
//            for (int j = 0; j < tthread; j++) {
//                if (i >= throughputs.get(j).length || i >= latencys.get(j).length) {
//                    break loop;
//                }
//                throughput = throughput + throughputs.get(j)[i];
//                latency = latency + latencys.get(j)[i];
//            }
//            periodicalLatency.add(latency/1E3); // ms to s
//            periodicalThroughput.add(throughput);
//        }
//
//        job.setLatency(periodicalLatency.stream().mapToDouble(Double::doubleValue).average().orElse(0.0));  // s
//        job.setPeriodicalLatency(periodicalLatency);
//        job.setPeriodicalThroughput(periodicalThroughput);
//
//        try {
//            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(job);
//            String jsonDirectory = "C:\\Users\\siqxi\\data\\job\\";
//            File file = new File(jsonDirectory + job.getAppId() + ".json");
//            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), CREATE, TRUNCATE_EXISTING);
//            fileWriter.write(json);
//            fileWriter.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    private static void AverageTotalTimeBreakdownReport(int tthread, int snapshotInterval) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("AverageTotalTimeBreakdownReport\n");
            if (enable_log) log.info("===Average Total Time Breakdown Report===");
            fileWriter.write("total_time\t serialize_time\t persist_time\t stream_process\t txn_process\t overheads\n");
            if (enable_log)
                log.info("total_time\t serialize_time\t persist_time\t stream_process\t txn_process\t overheads");
            double totalProcessTime = 0;
            double totalSerializeTime = 0;
            double totalPersistTime = 0;
            double totalStreamProcessTime = 0;
            double totalTxnProcessTime = 0;
            double totalOverheads = 0;
            for (int threadId = 0; threadId < tthread; threadId++) {
                totalProcessTime += Metrics.Total_Record.totalProcessTimePerEvent[threadId].getMean();
                if (snapshotInterval != 0) {
                    totalSerializeTime += Metrics.Total_Record.compression_total[threadId].getMean() + Metrics.Total_Record.serialization_total[threadId].getMean() + Metrics.Total_Record.snapshot_serialization_total[threadId].getMean() / snapshotInterval;
                }
                totalPersistTime += Metrics.Total_Record.persist_total[threadId].getMean();
                totalStreamProcessTime += Metrics.Total_Record.stream_total[threadId].getMean();
                totalTxnProcessTime += Metrics.Total_Record.txn_total[threadId].getMean();
                totalOverheads += Metrics.Total_Record.overhead_total[threadId].getMean();
            }
            String output = String.format(
                    "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f"
                    , (totalProcessTime + totalSerializeTime + totalPersistTime) / tthread
                    , totalSerializeTime / tthread
                    , totalPersistTime / tthread
                    , totalStreamProcessTime / tthread
                    , totalTxnProcessTime / tthread
                    , totalOverheads / tthread
            );
            fileWriter.write(output + "\n");
            if (enable_log) log.info(output);
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void WriteThroughputPerPhase(int tthread, int shiftRate) {
        try {
            double[] tr = new double[Metrics.Runtime.ThroughputPerPhase.get(0).size()];
            //every punctuation
            for (int i = 0; i < tr.length; i++) {
                tr[i] = 0;
                for (int j = 0; j < tthread; j++) {
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
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("phase_id\t throughput\n");
            for (int i = 0; i < tr_p.length; i++) {
                String output = String.format("%d\t" +
                                "%-10.4f\t"
                        , i, tr_p[i]
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

    public static void WritePersistFileSize(int ftOption, int tthread) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            if (ftOption == FTOption_ISC || ftOption == FTOption_WSC || ftOption == FTOption_PATH || ftOption == FTOption_Dependency || ftOption == FTOption_Command) {
                fileWriter.write("SnapshotSizeReport (KB): " + "\n");
                fileWriter.write("thread_id" + "\t" + "size" + "\n");
                double totalSize = 0;
                for (int i = 0; i < tthread; i++) {
                    totalSize = totalSize + Metrics.RuntimePerformance.SnapshotSize[i].getMean();
                    fileWriter.write(i + "\t" + Metrics.RuntimePerformance.SnapshotSize[i].getMean() + "\n");
                }
                fileWriter.write("SnapshotTotalSize (KB): " + totalSize + "\n");
            }
            if (ftOption == FTOption_WSC) {
                fileWriter.write("WriteAheadLogSize: " + "\n");
                fileWriter.write("thread_id" + "\t" + "size (KB)" + "\n");
                double totalSize = 0;
                for (int i = 0; i < tthread; i++) {
                    totalSize = totalSize + Metrics.RuntimePerformance.LogSize[i].getMean();
                    fileWriter.write(i + "\t" + Metrics.RuntimePerformance.LogSize[i].getMean() + "\n");
                }
                fileWriter.write("WriteAheadLogTotalSize (KB): " + totalSize + "\n");
            }
            if (ftOption == FTOption_PATH) {
                fileWriter.write("PathLogSize: " + "\n");
                fileWriter.write("thread_id" + "\t" + "size (KB)" + "\n");
                double totalSize = 0;
                for (int i = 0; i < tthread; i++) {
                    totalSize = totalSize + Metrics.RuntimePerformance.LogSize[i].getMean();
                    fileWriter.write(i + "\t" + Metrics.RuntimePerformance.LogSize[i].getMean() + "\n");
                }
                fileWriter.write("PathLogTotalSize (KB): " + totalSize + "\n");
            }
            if (ftOption == FTOption_Dependency) {
                fileWriter.write("DependencyLogSize: " + "\n");
                fileWriter.write("thread_id" + "\t" + "size (KB)" + "\n");
                double totalSize = 0;
                for (int i = 0; i < tthread; i++) {
                    totalSize = totalSize + Metrics.RuntimePerformance.LogSize[i].getMean();
                    fileWriter.write(i + "\t" + Metrics.RuntimePerformance.LogSize[i].getMean() + "\n");
                }
                fileWriter.write("DependencyTotalSize (KB): " + totalSize + "\n");
            }
            if (ftOption == FTOption_LV) {
                fileWriter.write("LSNVectorLoggingSize: " + "\n");
                fileWriter.write("thread_id" + "\t" + "size (KB)" + "\n");
                double totalSize = 0;
                for (int i = 0; i < tthread; i++) {
                    totalSize = totalSize + Metrics.RuntimePerformance.LogSize[i].getMean();
                    fileWriter.write(i + "\t" + Metrics.RuntimePerformance.LogSize[i].getMean() + "\n");
                }
                fileWriter.write("LSNVectorLoggingSize (KB): " + totalSize + "\n");
            }
            if (ftOption == FTOption_LV) {
                fileWriter.write("CommandLoggingSize: " + "\n");
                fileWriter.write("thread_id" + "\t" + "size (KB)" + "\n");
                double totalSize = 0;
                for (int i = 0; i < tthread; i++) {
                    totalSize = totalSize + Metrics.RuntimePerformance.LogSize[i].getMean();
                    fileWriter.write(i + "\t" + Metrics.RuntimePerformance.LogSize[i].getMean() + "\n");
                }
                fileWriter.write("CommandLoggingSize (KB): " + totalSize + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteRecoveryTime(int tthread, int FTOption) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("Recovery Overall: " + "\n");
            double totalRecoveryTime = 0;
            long totalRecoveryItemsCount = 0;
            for (int i = 0; i < tthread; i++) {
                totalRecoveryTime = totalRecoveryTime + Metrics.RecoveryPerformance.RecoveryTime[i].getMean();
                totalRecoveryItemsCount = totalRecoveryItemsCount + Metrics.RecoveryPerformance.recoveryItems[i];
            }
            fileWriter.write("recoveryTime (ms) \t recoveryCount\t throughput (k/s) \n");
            fileWriter.write(totalRecoveryTime / tthread + "\t" + totalRecoveryItemsCount / tthread + "\t" + totalRecoveryItemsCount / totalRecoveryTime + "\n");
            double[] recoveryTime = new double[14];
            WriteRecoveryTimeBreakDown(tthread, recoveryTime);
            WriteReplayTimeBreakDown(tthread, recoveryTime);
            WriteRecoverySchedulerTimeBreakdownReport(tthread, recoveryTime);
            fileWriter.write("OverallBreakDownReport (ms): " + "\n");
            fileWriter.write("Reload\t Explore\t Execute\t Abort\t Construct\t Wait\n");
            recoveryTime[0] = 0;
            if (FTOption == FTOption_ISC) {
                String output = String.format(
                        "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , recoveryTime[0] + recoveryTime[2], recoveryTime[4], recoveryTime[3] + recoveryTime[6], recoveryTime[7], recoveryTime[5], recoveryTime[8] + recoveryTime[9]);
                fileWriter.write(output + "\n");
            }
            if (FTOption == FTOption_PATH) {
                String output = String.format(
                        "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , recoveryTime[0] + recoveryTime[1] + recoveryTime[2] + recoveryTime[10], recoveryTime[4] + recoveryTime[12], recoveryTime[3] + recoveryTime[6], recoveryTime[7], recoveryTime[5] + recoveryTime[11], recoveryTime[8] + recoveryTime[9]);
                fileWriter.write(output + "\n");
            }
            if (FTOption == FTOption_LV) {
                String output = String.format(
                        "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , recoveryTime[0] + recoveryTime[2] + recoveryTime[5], recoveryTime[4], recoveryTime[6], recoveryTime[7], 0.0, recoveryTime[1] - recoveryTime[4] - recoveryTime[5] - recoveryTime[6]);
                fileWriter.write(output + "\n");
            }
            if (FTOption == FTOption_Dependency) {
                String output = String.format(
                        "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , recoveryTime[0] + recoveryTime[2] + recoveryTime[5], recoveryTime[4], recoveryTime[6], recoveryTime[7], recoveryTime[13], recoveryTime[1] - recoveryTime[4] - recoveryTime[5] - recoveryTime[6] - recoveryTime[13]);
                fileWriter.write(output + "\n");
            }
            if (FTOption == FTOption_Command) {
                String output = String.format(
                        "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , recoveryTime[0] + recoveryTime[2] + recoveryTime[5], recoveryTime[4], recoveryTime[6], recoveryTime[7], 0.0, recoveryTime[1] - recoveryTime[4] - recoveryTime[5] - recoveryTime[6]);
                fileWriter.write(output + "\n");
            }
            fileWriter.write("reloadDatabaseTime\t redoWriteAheadLogTime\t reloadInputTime\t stream_process\t explore_time\t construct_time\t useful_time\t abort_time\t overheads\t abort_push_down\t history_inspect\t tasking_placing\t construct_graph\n");
            String output = String.format(
                    "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , recoveryTime[0], recoveryTime[1], recoveryTime[2], recoveryTime[3], recoveryTime[4], recoveryTime[5], recoveryTime[6], recoveryTime[7], recoveryTime[8], recoveryTime[9], recoveryTime[10], recoveryTime[11], recoveryTime[12]
            );
            fileWriter.write(output + "\n");
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteRecoveryTimeBreakDown(int tthread, double[] recoveryTime) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("RecoveryTimeBreakDownReport (ms): " + "\n");
            fileWriter.write("reloadDatabaseTime\t redoWriteAheadLogTime\t reloadInputTime\t replayTime\n");
            double totalReloadDatabaseTime = 0;
            double totalRedoWriteAheadLogTime = 0;
            double totalReloadInputTime = 0;
            double totalReplayTime = 0;
            for (int threadId = 0; threadId < tthread; threadId++) {
                totalReloadDatabaseTime = totalReloadDatabaseTime + Metrics.RecoveryPerformance.ReloadDatabaseTime[threadId].getMean();
                if (Metrics.RecoveryPerformance.RedoWriteAheadLogTime[threadId] != null)
                    totalRedoWriteAheadLogTime = totalRedoWriteAheadLogTime + Metrics.RecoveryPerformance.RedoWriteAheadLogTime[threadId].getMean();
                totalReloadInputTime = totalReloadInputTime + Metrics.RecoveryPerformance.ReloadInputTime[threadId].getMean();
                totalReplayTime = totalReplayTime + Metrics.RecoveryPerformance.ReplayTime[threadId].getMean();
            }
            String output = String.format(
                    "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , totalReloadDatabaseTime / tthread
                    , totalRedoWriteAheadLogTime / tthread
                    , totalReloadInputTime / tthread
                    , totalReplayTime / tthread
            );
            fileWriter.write(output + "\n");
            fileWriter.close();
            recoveryTime[0] = totalReloadDatabaseTime / tthread;
            recoveryTime[1] = totalRedoWriteAheadLogTime / tthread;
            recoveryTime[2] = totalReloadInputTime / tthread;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteReplayTimeBreakDown(int tthread, double[] recoveryTime) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("ReplayTimeBreakDownReport (ms): " + "\n");
            fileWriter.write("total_time\t stream_process\t txn_process\t overheads\n");
            double totalReplayTime = 0;
            double totalStreamProcessTime = 0;
            double totalTxnProcessTime = 0;
            double totalOverheads = 0;
            for (int threadId = 0; threadId < tthread; threadId++) {
                totalReplayTime = totalReplayTime + Metrics.RecoveryPerformance.total_time[threadId];
                totalStreamProcessTime = totalStreamProcessTime + Metrics.RecoveryPerformance.stream_total[threadId];
                totalTxnProcessTime = totalTxnProcessTime + Metrics.RecoveryPerformance.txn_total[threadId];
                totalOverheads = totalOverheads + Metrics.RecoveryPerformance.overhead_total[threadId];
            }
            String output = String.format(
                    "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , totalReplayTime / tthread / 1E6
                    , totalStreamProcessTime / tthread / 1E6
                    , totalTxnProcessTime / tthread / 1E6
                    , totalOverheads / tthread / 1E6
            );
            fileWriter.write(output + "\n");
            fileWriter.close();
            recoveryTime[3] = totalStreamProcessTime / tthread / 1E6;
            recoveryTime[8] = totalOverheads / tthread / 1E6;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteRecoverySchedulerTimeBreakdownReport(int tthread, double[] recoveryTime) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("RecoverySchedulerTimeBreakdownReport (ms): " + "\n");
            fileWriter.write("explore_time\t useful_time\t abort_time\t construct_time\t abort_push\t history_inspection\t task_placing\n");
            double totalExploreTime = 0;
            double totalUsefulTime = 0;
            double totalAbortTime = 0;
            double totalConstructTime = 0;
            double totalAbortPushTime = 0;
            double totalWaitTime = 0;
            double totalHistoryInspectionTime = 0;
            double totalTaskPlacingTime = 0;
            double totalConstructGraphTime = 0;
            for (int threadId = 0; threadId < tthread; threadId++) {
                totalAbortPushTime = totalAbortPushTime + Metrics.RecoveryPerformance.AbortPushDown[threadId];
                totalHistoryInspectionTime = totalHistoryInspectionTime + Metrics.RecoveryPerformance.HistoryInspection[threadId];
                totalTaskPlacingTime = totalTaskPlacingTime + Metrics.RecoveryPerformance.TaskPlacing[threadId];
                totalConstructGraphTime = totalConstructGraphTime + Metrics.RecoveryPerformance.ConstructGraph[threadId];
                totalExploreTime = totalExploreTime + Metrics.RecoveryPerformance.Explore[threadId];
                totalUsefulTime = totalUsefulTime + Metrics.RecoveryPerformance.Useful[threadId];
                totalAbortTime = totalAbortTime + Metrics.RecoveryPerformance.Abort[threadId];
                totalConstructTime = totalConstructTime + Metrics.RecoveryPerformance.Construct[threadId];
                totalWaitTime = totalWaitTime + Metrics.RecoveryPerformance.Wait[threadId];
            }
            String output = String.format(
                    "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , (totalExploreTime - totalTaskPlacingTime - totalConstructGraphTime) / tthread / 1E6
                    , totalUsefulTime / tthread / 1E6
                    , totalAbortTime / tthread / 1E6
                    , (totalConstructTime - totalHistoryInspectionTime) / tthread / 1E6
                    , totalAbortPushTime / tthread / 1E6
                    , totalHistoryInspectionTime / tthread / 1E6
                    , totalTaskPlacingTime / tthread / 1E6
                    , totalConstructGraphTime / tthread / 1E6
                    , totalWaitTime / tthread / 1E6
            );
            fileWriter.write(output + "\n");
            fileWriter.close();
            recoveryTime[4] = (totalExploreTime - totalTaskPlacingTime - totalConstructGraphTime) / tthread / 1E6;
            recoveryTime[5] = (totalConstructTime - totalHistoryInspectionTime) / tthread / 1E6;
            recoveryTime[6] = totalUsefulTime / tthread / 1E6;
            recoveryTime[7] = totalAbortTime / tthread / 1E6;
            recoveryTime[9] = totalWaitTime / tthread / 1E6;
            recoveryTime[10] = totalAbortPushTime / tthread / 1E6;
            recoveryTime[11] = totalHistoryInspectionTime / tthread / 1E6;
            recoveryTime[12] = totalTaskPlacingTime / tthread / 1E6;
            recoveryTime[13] = totalConstructGraphTime / tthread / 1E6;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void SchedulerTimeBreakdownReport(int tthread) {
        try {
            if (enable_debug) log.info("++++++ counter: " + counter);
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("SchedulerTimeBreakdownReport (ns)\n");
            if (enable_log) log.info("===Scheduler Time Breakdown Report===");
            fileWriter.write("explore_time\t useful_time\t abort_time\t construct_time\t tracking_time\n");
            if (enable_log)
                log.info("explore_time\t useful_time\t abort_time\t construct_time\t tracking_time");
            double explore_time = 0;
            double useful_time = 0;
            double abort_time = 0;
            double construct_time = 0;
            double tracking_time = 0;
            for (int threadId = 0; threadId < tthread; threadId++) {
                explore_time += Metrics.Scheduler_Record.Explore[threadId].getMean();
                useful_time += Metrics.Scheduler_Record.Useful[threadId].getMean();
                abort_time += Metrics.Scheduler_Record.Abort[threadId].getMean();
                construct_time += Metrics.Scheduler_Record.Construct[threadId].getMean();
                tracking_time += Metrics.Scheduler_Record.Tracking[threadId].getMean();
            }
            String output = String.format(
                    "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , explore_time / tthread
                    , useful_time / tthread
                    , abort_time / tthread
                    , construct_time / tthread
                    , tracking_time / tthread
            );
            if (enable_log) log.info(output);
            fileWriter.write(output + "\n");
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void TransactionBreakdownRatioReport(int ccOption, int tthread) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("TransactionBreakdownRatioReport (ns)\n");
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
                        , Metrics.Transaction_Record.index_ratio[threadId].getMean()
                        , Metrics.Transaction_Record.useful_ratio[threadId].getMean()
                        , Metrics.Transaction_Record.sync_ratio[threadId].getMean()
                        , ccOption == CCOption_MorphStream ? 0 : Metrics.Transaction_Record.lock_ratio[threadId].getMean()
                );
                fileWriter.write(output + "\n");
                if (enable_log) log.info(output);
            }
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void WriteRuntimePerformance(int tthread) {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".runtime");
            file.mkdirs();
            if (file.exists()) {
                try {
                    file.delete();
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));
            fileWriter.write("throughput\t latency\n");
            List<double[]> latencys = new ArrayList<>();
            List<double[]> throughputs = new ArrayList<>();
            for (int i = 0; i < tthread; i++) {
                latencys.add(Metrics.RuntimePerformance.Latency[i].getValues());
                throughputs.add(Metrics.RuntimePerformance.Throughput[i].getValues());
            }
            loop:
            for (int i = 0; i < latencys.get(0).length; i++) {
                double throughput = 0;
                double latency = 0;
                for (int j = 0; j < tthread; j++) {
                    if (i >= throughputs.get(j).length || i >= latencys.get(j).length) {
                        break loop;
                    }
                    throughput = throughput + throughputs.get(j)[i];
                    latency = latency + latencys.get(j)[i];
                }
                fileWriter.write(throughput + "\t" + latency / tthread + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteLastTasks(String rootFile, int tthread) {
        try {
            for (int i = 0; i < tthread; i++) {
                File file = new File(rootFile + OsUtils.OS_wrapper("outputStore") + OsUtils.OS_wrapper(i + ".output"));
                file.mkdirs();
                if (file.exists()) {
                    try {
                        file.delete();
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));
                fileWriter.write(String.valueOf(Metrics.RuntimePerformance.lastTasks[i]));
                fileWriter.flush();
                fileWriter.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteMemoryConsumption() {
        if (enable_memory_measurement) {
            Metrics.timer.cancel();
            try {
                File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".memory");
                file.mkdirs();
                if (file.exists()) {
                    try {
                        file.delete();
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                BufferedWriter w = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
                w.write("UsedMemory\n");
                for (int i = 0; i < Metrics.usedMemory.getValues().length; i++) {
                    String output = String.format("%f\t" +
                                    "%-10.4f\t"
                            , (float) i, Metrics.usedMemory.getValues()[i]);
                    w.write(output + "\n");
                }
                w.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void WriteSSDConsumption() {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".ssd");
            file.mkdirs();
            if (file.exists()) {
                try {
                    file.delete();
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            BufferedWriter w = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            w.write("IncreaseFileSize (KB):\n");
            for (int i = 0; i < Metrics.usedFileSize.getValues().length; i++) {
                String output = String.format("%f\t" +
                                "%-10.4f\t"
                        , (float) i, Metrics.usedFileSize.getValues()[i]);
                w.write(output + "\n");
            }
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void WriteSSDBandwidth() {
        try {
            File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".bandwidth");
            file.mkdirs();
            if (file.exists()) {
                try {
                    file.delete();
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            BufferedWriter w = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            w.write("Bandwidth (kb/s):\n");
            for (int i = 0; i < Metrics.SSDBandwidth.getValues().length; i ++){
                String output = String.format("%f\t" +
                                "%-10.4f\t"
                        , (float) i ,Metrics.SSDBandwidth.getValues()[i]);
                w.write(output + "\n");
            }
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void METRICS_REPORT(Configuration config, int tthread, double throughput) {
        int ccOption = config.getInt("CCOption", 0);
        int FTOption = config.getInt("FTOption", 0);
//        int phase = config.getInt("phaseNum");
//        int shiftRate = config.getInt("shiftRate");
        int snapshotInterval = config.getInt("snapshotInterval");

        WriteThroughputReport(throughput);
        AverageTotalTimeBreakdownReport(tthread, snapshotInterval);
        WritePersistFileSize(FTOption, tthread);
        //WriteThroughputPerPhase(tthread, phase, shiftRate);
        if (Metrics.fileNameSuffix.equals("_recovery")) {
            WriteRecoveryTime(tthread, FTOption);
        }
        if (ccOption == CCOption_MorphStream) {//extra info
            SchedulerTimeBreakdownReport(tthread);
        } else {
            TransactionBreakdownRatioReport(ccOption, tthread);
        }
        WriteRuntimePerformance(tthread);
        WriteMemoryConsumption();
        WriteSSDConsumption();
        WriteSSDBandwidth();

        WriteJSONData(config, tthread, throughput, snapshotInterval);
    }

    public static void METRICS_REPORT_WITH_FAILURE(int ccOption, int FTOption, int tthread, String rootFile, int snapshotInterval) {
        File file = new File(Metrics.directory + Metrics.fileNameSuffix + ".overall");
        file.mkdirs();
        if (file.exists()) {
            try {
                file.delete();
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        AverageTotalTimeBreakdownReport(tthread, snapshotInterval);
        WritePersistFileSize(FTOption, tthread);
        WriteRuntimePerformance(tthread);
        if (ccOption == CCOption_MorphStream) {//extra info
            SchedulerTimeBreakdownReport(tthread);
        } else {
            TransactionBreakdownRatioReport(ccOption, tthread);
        }
        WriteRuntimePerformance(tthread);
        WriteMemoryConsumption();
        WriteSSDConsumption();
        WriteSSDBandwidth();
        WriteLastTasks(rootFile, tthread);
    }
}
