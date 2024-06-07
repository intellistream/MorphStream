package intellistream.morphstream.engine.txn.profiler;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.util.OsUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.*;

public class MeasureTools {
    private static final Logger log = LoggerFactory.getLogger(MeasureTools.class);
    private static String MetricsDirectory;

    public static void Initialize(Configuration configuration) {
        MetricsDirectory = configuration.getString("rootPath") + OsUtils.OS_wrapper("metrics") + OsUtils.OS_wrapper(configuration.getString("clientClassName") + OsUtils.OS_wrapper(configuration.getString("scheduler")));
        Metrics.DriverRuntime.Initialize();
        Metrics.WorkerRuntime.Initialize();
    }
    //Driver Metrics
    public static void DriverPrepareStartTime(int threadId) {
        Metrics.DriverRuntime.prepareStartTime[threadId] = System.nanoTime();
    }
    public static void DriverPrepareEndTime(int threadId) {
        Metrics.DriverRuntime.prepareEndTime[threadId] = System.nanoTime();
        Metrics.DriverRuntime.prepareTimeStatistics[threadId].addValue(Metrics.DriverRuntime.prepareEndTime[threadId] - Metrics.DriverRuntime.prepareStartTime[threadId]);
    }
    public static void DriverRdmaStartSendEventTime(int threadId) {
        Metrics.DriverRuntime.rdmaStartSendEventTime[threadId] = System.nanoTime();
    }
    public static void DriverRdmaEndSendEventTime(int threadId) {
        Metrics.DriverRuntime.rdmaEndSendEventTime[threadId] = System.nanoTime();
        Metrics.DriverRuntime.rdmaSendTimeStatistics[threadId].addValue(Metrics.DriverRuntime.rdmaEndSendEventTime[threadId] - Metrics.DriverRuntime.rdmaStartSendEventTime[threadId]);
    }
    public static void DriverRdmaStartRecvEventTime(int threadId) {
        Metrics.DriverRuntime.rdmaStartRecvEventTime[threadId] = System.nanoTime();
    }
    public static void DriverRdmaEndRecvEventTime(int threadId) {
        Metrics.DriverRuntime.rdmaEndRecvEventTime[threadId] = System.nanoTime();
        Metrics.DriverRuntime.rdmaRecvTimeStatistics[threadId].addValue(Metrics.DriverRuntime.rdmaEndRecvEventTime[threadId] - Metrics.DriverRuntime.rdmaStartRecvEventTime[threadId]);
    }
    public static void DriverFinishStartTime(int threadId) {
        Metrics.DriverRuntime.finishStartTime[threadId] = System.nanoTime();
    }
    public static void DriverFinishEndTime(int threadId) {
        Metrics.DriverRuntime.finishEndTime[threadId] = System.nanoTime();
        Metrics.DriverRuntime.finishTimeStatistics[threadId].addValue(Metrics.DriverRuntime.finishEndTime[threadId] - Metrics.DriverRuntime.finishStartTime[threadId]);
    }
    //Worker Metrics
    public static void WorkerRdmaRecvStartEventTime(int threadId) {
        Metrics.WorkerRuntime.rdmaRecvStartEventTime[threadId] = System.nanoTime();
    }
    public static void WorkerRdmaRecvEndEventTime(int threadId) {
        Metrics.WorkerRuntime.rdmaRecvEndEventTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.rdmaRecvTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.rdmaRecvEndEventTime[threadId] - Metrics.WorkerRuntime.rdmaRecvStartEventTime[threadId]);
    }
    public static void WorkerPrepareStartTime(int threadId) {
        Metrics.WorkerRuntime.prepareStartTime[threadId] = System.nanoTime();
    }
    public static void WorkerPrepareEndTime(int threadId) {
        Metrics.WorkerRuntime.prepareEndTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.prepareTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.prepareEndTime[threadId] - Metrics.WorkerRuntime.prepareStartTime[threadId]);
    }
    public static void WorkerRdmaRecvOwnershipTableStartEventTime(int threadId) {
        Metrics.WorkerRuntime.rdmaRecvStartOwnershipTableTime[threadId] = System.nanoTime();
    }
    public static void WorkerRdmaRecvOwnershipTableEndEventTime(int threadId) {
        Metrics.WorkerRuntime.rdmaRecvEndOwnershipTableTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.rdmaRecvOwnershipTableTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.rdmaRecvEndOwnershipTableTime[threadId] - Metrics.WorkerRuntime.rdmaRecvStartOwnershipTableTime[threadId]);
    }
    public static void WorkerPrepareCacheStartTime(int threadId) {
        Metrics.WorkerRuntime.prepareCacheStartTime[threadId] = System.nanoTime();
    }
    public static void WorkerPrepareCacheEndTime(int threadId) {
        Metrics.WorkerRuntime.prepareCacheEndTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.prepareCacheTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.prepareCacheEndTime[threadId] - Metrics.WorkerRuntime.prepareCacheStartTime[threadId]);
    }
    public static void WorkerRdmaRemoteOperationStartEventTime(int threadId) {
        Metrics.WorkerRuntime.rdmaStartRemoteOperationTime[threadId] = System.nanoTime();
    }
    public static void WorkerRdmaRemoteOperationEndEventTime(int threadId) {
        Metrics.WorkerRuntime.rdmaEndRemoteOperationTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.rdmaRemoteOperationTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.rdmaEndRemoteOperationTime[threadId] - Metrics.WorkerRuntime.rdmaStartRemoteOperationTime[threadId]);
    }
    public static void WorkerSetupDependenciesStartEventTime(int threadId) {
        Metrics.WorkerRuntime.setupDependenciesStartTime[threadId] = System.nanoTime();
    }
    public static void WorkerSetupDependenciesEndEventTime(int threadId) {
        Metrics.WorkerRuntime.setupDependenciesEndTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.setupDependenciesTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.setupDependenciesEndTime[threadId] - Metrics.WorkerRuntime.setupDependenciesStartTime[threadId]);
    }
    public static void WorkerExecuteStartEventTime(int threadId) {
        Metrics.WorkerRuntime.executeStartTime[threadId] = System.nanoTime();
    }
    public static void WorkerExecuteEndEventTime(int threadId) {
        Metrics.WorkerRuntime.executeEndTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.executeTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.executeEndTime[threadId] - Metrics.WorkerRuntime.executeStartTime[threadId]);
    }
    public static void WorkerFinishStartTime(int threadId) {
        Metrics.WorkerRuntime.finishStartTime[threadId] = System.nanoTime();
    }
    public static void WorkerFinishEndTime(int threadId) {
        Metrics.WorkerRuntime.finishEndTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.finishTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.finishEndTime[threadId] - Metrics.WorkerRuntime.finishStartTime[threadId]);
    }
    public static void WorkerRdmaSendResultStartTime(int threadId) {
        Metrics.WorkerRuntime.rdmaSendResultStartTime[threadId] = System.nanoTime();
    }
    public static void WorkerRdmaSendResultEndTime(int threadId) {
        Metrics.WorkerRuntime.rdmaSendResultEndTime[threadId] = System.nanoTime();
        Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[threadId].addValue(Metrics.WorkerRuntime.rdmaSendResultEndTime[threadId] - Metrics.WorkerRuntime.rdmaSendResultStartTime[threadId]);
    }
    public static void WorkerRdmaRound(int threadId, int round) {
        Metrics.WorkerRuntime.rdmaRounds[threadId].addValue(round);
    }
    private static void WriteThroughput(double throughput) {
        try {
            File file = new File(MetricsDirectory + "overall.txt");
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
            fileWriter.write("Throughput (k DAGs/s): " + throughput + "\n");
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void WriteLatency(DescriptiveStatistics descriptiveStatistics) {
        File file = new File(MetricsDirectory + "overall.txt");
        try {
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("Avg Latency: " + descriptiveStatistics.getMean() + "\n");
            fileWriter.write("99th Latency: " + descriptiveStatistics.getPercentile(99) + "\n");
            Double[] percentile = {10.0, 30.0, 50.0, 70.0, 90.0};
            for (Double p : percentile) {
                fileWriter.write( p + "th Latency: " + descriptiveStatistics.getPercentile(p) + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static void WriteDriverMetrics(int frontendNumber) {
        try {
            File file = new File(MetricsDirectory + "driver.txt");
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
            fileWriter.write("Driver Metrics: " + "\n");
            fileWriter.write("thread_id\t prepareTime (s)\t rdmaSendTime (s)\t rdmaRecvTime (s)\t finishTime (s)\n");
            double totalPrepareTime = 0;
            double totalRdmaSendTime = 0;
            double totalRdmaRecvTime = 0;
            double totalFinishTime = 0;
            for (int i = 0; i < frontendNumber; i++) {
                totalPrepareTime = totalPrepareTime + Metrics.DriverRuntime.prepareTimeStatistics[i].getSum();
                totalRdmaSendTime = totalRdmaSendTime + Metrics.DriverRuntime.rdmaSendTimeStatistics[i].getSum();
                totalRdmaRecvTime = totalRdmaRecvTime + Metrics.DriverRuntime.rdmaRecvTimeStatistics[i].getSum();
                totalFinishTime = totalFinishTime + Metrics.DriverRuntime.finishTimeStatistics[i].getSum();
                fileWriter.write(i + "\t" + Metrics.DriverRuntime.prepareTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.DriverRuntime.rdmaSendTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.DriverRuntime.rdmaRecvTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.DriverRuntime.finishTimeStatistics[i].getSum() / 1E9 + "\n");
            }
            double totalDriverTime = totalPrepareTime + totalRdmaSendTime + totalRdmaRecvTime + totalFinishTime;
            fileWriter.write("Breakdown in percentage: " + "\n");
            fileWriter.write("Prepare (%)\t RdmaSend (%)\t RdmaRecv (%)\t Finish (%)\n");
            fileWriter.write(totalPrepareTime / totalDriverTime + "\t" + totalRdmaSendTime / totalDriverTime + "\t" + totalRdmaRecvTime / totalDriverTime + "\t" + totalFinishTime / totalDriverTime + "\n");
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteWorkerMetrics(int tthread) {
        try {
            File file = new File(MetricsDirectory + "driver.txt");
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
            fileWriter.write("Worker Metrics: " + "\n");
            fileWriter.write("thread_id\t rdmaRecvTime (s)\t prepareTime (s)\t prepareCacheTime (s)\t rdmaRemoteOperationTime (s)\t setupDependenciesTime (s)\t executeTime (s)\t finishTime (s)\t rdmaSendResultTime (s)\n");
            double totalRdmaRecvTime = 0;
            double totalPrepareTime = 0;
            double totalPrepareCacheTime = 0;
            double totalRdmaRemoteOperationTime = 0;
            double totalSetupDependenciesTime = 0;
            double totalExecuteTime = 0;
            double totalFinishTime = 0;
            double totalRdmaSendResultTime = 0;
            double totalRounds = 0;
            double avgRounds = 0;
            for (int i = 0; i < tthread; i++) {
                totalRdmaRecvTime = totalRdmaRecvTime + Metrics.WorkerRuntime.rdmaRecvTimeStatistics[i].getSum();
                totalPrepareTime = totalPrepareTime + Metrics.WorkerRuntime.prepareTimeStatistics[i].getSum();
                totalPrepareCacheTime = totalPrepareCacheTime + Metrics.WorkerRuntime.prepareCacheTimeStatistics[i].getSum();
                totalRdmaRemoteOperationTime = totalRdmaRemoteOperationTime + Metrics.WorkerRuntime.rdmaRemoteOperationTimeStatistics[i].getSum();
                totalSetupDependenciesTime = totalSetupDependenciesTime + Metrics.WorkerRuntime.setupDependenciesTimeStatistics[i].getSum();
                totalExecuteTime = totalExecuteTime + Metrics.WorkerRuntime.executeTimeStatistics[i].getSum();
                totalFinishTime = totalFinishTime + Metrics.WorkerRuntime.finishTimeStatistics[i].getSum();
                totalRdmaSendResultTime = totalRdmaSendResultTime + Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[i].getSum();
                fileWriter.write(i + "\t" + Metrics.WorkerRuntime.rdmaRecvTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.prepareTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.prepareCacheTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.rdmaRemoteOperationTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.setupDependenciesTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.executeTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.finishTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[i].getSum() / 1E9 + "\n");
            }
            double totalWorkerTime = totalRdmaRecvTime + totalPrepareTime + totalPrepareCacheTime + totalRdmaRemoteOperationTime + totalSetupDependenciesTime + totalExecuteTime + totalFinishTime + totalRdmaSendResultTime;
            fileWriter.write("Breakdown in percentage: " + "\n");
            fileWriter.write("RdmaRecv (%)\t Prepare (%)\t PrepareCache (%)\t RdmaRemoteOperation (%)\t SetupDependencies (%)\t Execute (%)\t Finish (%)\t RdmaSendResult (%)\n");
            fileWriter.write(totalRdmaRecvTime / totalWorkerTime + "\t" + totalPrepareTime / totalWorkerTime + "\t" + totalPrepareCacheTime / totalWorkerTime + "\t" + totalRdmaRemoteOperationTime / totalWorkerTime + "\t" + totalSetupDependenciesTime / totalWorkerTime + "\t" + totalExecuteTime / totalWorkerTime + "\t" + totalFinishTime / totalWorkerTime + "\t" + totalRdmaSendResultTime / totalWorkerTime + "\n");
            fileWriter.write("Communication Rounds: " + "\n");
            fileWriter.write("thread_id\t Total Rounds\t Avg Rounds\n");
            for (int i = 0; i < tthread; i++) {
                totalRounds = totalRounds + Metrics.WorkerRuntime.rdmaRounds[i].getSum();
                avgRounds = avgRounds + Metrics.WorkerRuntime.rdmaRounds[i].getMean();
                fileWriter.write(i + "\t" + Metrics.WorkerRuntime.rdmaRounds[i].getSum() + "\t" + Metrics.WorkerRuntime.rdmaRounds[i].getMean() + "\n");
            }
            fileWriter.write("Total Rounds: " + totalRounds + "\n");
            fileWriter.write("Avg Rounds: " + avgRounds / tthread + "\n");
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void DRIVER_METRICS_REPORT(int frontendNumber, double throughput, DescriptiveStatistics latencyStatistics) {
        WriteThroughput(throughput);
        WriteLatency(latencyStatistics);
        WriteDriverMetrics(frontendNumber);
    }
    public static void WORKER_METRICS_REPORT(int tthread) {
        WriteWorkerMetrics(tthread);
    }
}
