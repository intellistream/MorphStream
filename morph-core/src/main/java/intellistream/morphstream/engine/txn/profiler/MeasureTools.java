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
        Metrics.DSRuntime.Initialize();
        Metrics.RemoteLockRuntime.Initialize();
        Metrics.RemoteOCCRuntime.Initialize();
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

    //DL Runtime
    public static void DSRdmaRecvOwnershipTableStartEventTime(int threadId) {
        Metrics.DSRuntime.rdmaRecvStartOwnershipTableTime[threadId] = System.nanoTime();
    }
    public static void DSRdmaRecvOwnershipTableEndEventTime(int threadId) {
        Metrics.DSRuntime.rdmaRecvEndOwnershipTableTime[threadId] = System.nanoTime();
        Metrics.DSRuntime.rdmaRecvOwnershipTableTimeStatistics[threadId].addValue(Metrics.DSRuntime.rdmaRecvEndOwnershipTableTime[threadId] - Metrics.DSRuntime.rdmaRecvStartOwnershipTableTime[threadId]);
    }
    public static void DSPrepareCacheStartTime(int threadId) {
        Metrics.DSRuntime.prepareCacheStartTime[threadId] = System.nanoTime();
    }
    public static void DSPrepareCacheEndTime(int threadId) {
        Metrics.DSRuntime.prepareCacheEndTime[threadId] = System.nanoTime();
        Metrics.DSRuntime.prepareCacheTimeStatistics[threadId].addValue(Metrics.DSRuntime.prepareCacheEndTime[threadId] - Metrics.DSRuntime.prepareCacheStartTime[threadId]);
    }
    public static void DSRdmaRemoteOperationStartEventTime(int threadId) {
        Metrics.DSRuntime.rdmaStartRemoteOperationTime[threadId] = System.nanoTime();
    }
    public static void DSRdmaRemoteOperationEndEventTime(int threadId) {
        Metrics.DSRuntime.rdmaEndRemoteOperationTime[threadId] = System.nanoTime();
        Metrics.DSRuntime.rdmaRemoteOperationTimeStatistics[threadId].addValue(Metrics.DSRuntime.rdmaEndRemoteOperationTime[threadId] - Metrics.DSRuntime.rdmaStartRemoteOperationTime[threadId]);
    }
    public static void DSSetupDependenciesStartEventTime(int threadId) {
        Metrics.DSRuntime.setupDependenciesStartTime[threadId] = System.nanoTime();
    }
    public static void DSSetupDependenciesEndEventTime(int threadId) {
        Metrics.DSRuntime.setupDependenciesEndTime[threadId] = System.nanoTime();
        Metrics.DSRuntime.setupDependenciesTimeStatistics[threadId].addValue(Metrics.DSRuntime.setupDependenciesEndTime[threadId] - Metrics.DSRuntime.setupDependenciesStartTime[threadId]);
    }
    public static void DSExecuteStartEventTime(int threadId) {
        Metrics.DSRuntime.executeStartTime[threadId] = System.nanoTime();
    }
    public static void DSExecuteEndEventTime(int threadId) {
        Metrics.DSRuntime.executeEndTime[threadId] = System.nanoTime();
        Metrics.DSRuntime.executeTimeStatistics[threadId].addValue(Metrics.DSRuntime.executeEndTime[threadId] - Metrics.DSRuntime.executeStartTime[threadId]);
    }
    public static void DSRemoteAccessStartEventTime(int threadId) {
        Metrics.DSRuntime.rdmaAccessStartEventTime[threadId] = System.nanoTime();
    }
    public static void DSRemoteAccessEndEventTime(int threadId) {
        Metrics.DSRuntime.rdmaAccessEndEventTime[threadId] = System.nanoTime();
        Metrics.DSRuntime.rdmaAccessTimeStatistics[threadId].addValue(Metrics.DSRuntime.rdmaAccessEndEventTime[threadId] - Metrics.DSRuntime.rdmaAccessStartEventTime[threadId]);
    }
    public static void DSTotalExecutionTimeStartEventTime(int threadId) {
        Metrics.DSRuntime.totalExecutionStartTime[threadId] = System.nanoTime();
    }
    public static void DSTotalExecutionTimeEndEventTime(int threadId) {
        Metrics.DSRuntime.totalExecutionEndTime[threadId] = System.nanoTime();
        Metrics.DSRuntime.totalExecutionTimeStatistics[threadId].addValue(Metrics.DSRuntime.totalExecutionEndTime[threadId] - Metrics.DSRuntime.totalExecutionStartTime[threadId]);
    }
    public static void DSCommitTimeStartEventTime(int threadId) {
        Metrics.DSRuntime.commitStartTime[threadId] = System.nanoTime();
    }
    public static void DSCommitTimeEndEventTime(int threadId) {
        Metrics.DSRuntime.commitEndTime[threadId] = System.nanoTime();
        Metrics.DSRuntime.commitTimeStatistics[threadId].addValue(Metrics.DSRuntime.commitEndTime[threadId] - Metrics.DSRuntime.commitStartTime[threadId]);
    }

    //Remote Lock Runtime
    public static void lockRLStartTime(int threadId) {
        Metrics.RemoteLockRuntime.lockStartTime[threadId] = System.nanoTime();
    }
    public static void lockRLEndTime(int threadId) {
        Metrics.RemoteLockRuntime.lockEndTime[threadId] = System.nanoTime();
        Metrics.RemoteLockRuntime.lockTimeStatistics[threadId].addValue(Metrics.RemoteLockRuntime.lockEndTime[threadId] - Metrics.RemoteLockRuntime.lockStartTime[threadId]);
    }
    public static void rmaAndExecutionStartTime(int threadId) {
        Metrics.RemoteLockRuntime.rmaAndExecutionStartTime[threadId] = System.nanoTime();
    }
    public static void rmaAndExecutionEndTime(int threadId) {
        Metrics.RemoteLockRuntime.rmaAndExecutionEndTime[threadId] = System.nanoTime();
        Metrics.RemoteLockRuntime.rmaAndExecutionTimeStatistics[threadId].addValue(Metrics.RemoteLockRuntime.rmaAndExecutionEndTime[threadId] - Metrics.RemoteLockRuntime.rmaAndExecutionStartTime[threadId]);
        Metrics.RemoteLockRuntime.executeTimeStatistics[threadId].addValue(Metrics.RemoteLockRuntime.executeACCTime[threadId]);
        Metrics.RemoteLockRuntime.executeACCTime[threadId] = 0;
    }
    public static void executeRLStartTime(int threadId) {
        Metrics.RemoteLockRuntime.executeStartTime[threadId] = System.nanoTime();
    }
    public static void executeRLEndTime(int threadId) {
        Metrics.RemoteLockRuntime.executeEndTime[threadId] = System.nanoTime();
        Metrics.RemoteLockRuntime.executeACCTime[threadId] = Metrics.RemoteLockRuntime.executeACCTime[threadId] + (Metrics.RemoteLockRuntime.executeEndTime[threadId] - Metrics.RemoteLockRuntime.executeStartTime[threadId]);
    }
    public static void commitRLStartTime(int threadId) {
        Metrics.RemoteLockRuntime.commitStartTime[threadId] = System.nanoTime();
    }
    public static void commitRLEndTime(int threadId) {
        Metrics.RemoteLockRuntime.commitEndTime[threadId] = System.nanoTime();
        Metrics.RemoteLockRuntime.commitTimeStatistics[threadId].addValue(Metrics.RemoteLockRuntime.commitEndTime[threadId] - Metrics.RemoteLockRuntime.commitStartTime[threadId]);
    }
    public static void unlockRLStartEventTime(int threadId) {
        Metrics.RemoteLockRuntime.unlockStartTime[threadId] = System.nanoTime();
    }
    public static void unlockRLEndEventTime(int threadId) {
        Metrics.RemoteLockRuntime.unlockEndTime[threadId] = System.nanoTime();
        Metrics.RemoteLockRuntime.unlockTimeStatistics[threadId].addValue(Metrics.RemoteLockRuntime.unlockEndTime[threadId] - Metrics.RemoteLockRuntime.unlockStartTime[threadId]);
    }

    //RemoteOCC Runtime
    public static void rdmAndExecutionStartTime(int threadId) {
        Metrics.RemoteOCCRuntime.rmaAndExecutionStartTime[threadId] = System.nanoTime();
    }
    public static void rdmAndExecutionEndTime(int threadId) {
        Metrics.RemoteOCCRuntime.rmaAndExecutionEndTime[threadId] = System.nanoTime();
        Metrics.RemoteOCCRuntime.rmaAndExecutionTimeStatistics[threadId].addValue(Metrics.RemoteOCCRuntime.rmaAndExecutionEndTime[threadId] - Metrics.RemoteOCCRuntime.rmaAndExecutionStartTime[threadId]);
        Metrics.RemoteOCCRuntime.executeTimeStatistics[threadId].addValue(Metrics.RemoteOCCRuntime.executionACCTime[threadId]);
        Metrics.RemoteOCCRuntime.executionACCTime[threadId] = 0;
    }
    public static void executeOCCStartTime(int threadId) {
        Metrics.RemoteOCCRuntime.executeStartTime[threadId] = System.nanoTime();
    }
    public static void executeOCCEndTime(int threadId) {
        Metrics.RemoteOCCRuntime.executeEndTime[threadId] = System.nanoTime();
        Metrics.RemoteOCCRuntime.executionACCTime[threadId] = Metrics.RemoteOCCRuntime.executionACCTime[threadId] + (Metrics.RemoteOCCRuntime.executeEndTime[threadId] - Metrics.RemoteOCCRuntime.executeStartTime[threadId]);
    }
    public static void validateOCCStartTime(int threadId) {
        Metrics.RemoteOCCRuntime.validateStartTime[threadId] = System.nanoTime();
    }
    public static void validateOCCEndTime(int threadId) {
        Metrics.RemoteOCCRuntime.validateEndTime[threadId] = System.nanoTime();
        Metrics.RemoteOCCRuntime.validateTimeStatistics[threadId].addValue(Metrics.RemoteOCCRuntime.validateEndTime[threadId] - Metrics.RemoteOCCRuntime.validateStartTime[threadId]);
    }

    public static void commitOCCStartTime(int threadId) {
        Metrics.RemoteOCCRuntime.commitStartTime[threadId] = System.nanoTime();
    }
    public static void commitOCCEndTime(int threadId) {
        Metrics.RemoteOCCRuntime.commitEndTime[threadId] = System.nanoTime();
        Metrics.RemoteOCCRuntime.commitTimeStatistics[threadId].addValue(Metrics.RemoteOCCRuntime.commitEndTime[threadId] - Metrics.RemoteOCCRuntime.commitStartTime[threadId]);
    }
    public static void unlockOCCStartTime(int threadId) {
        Metrics.RemoteOCCRuntime.unlockStartTime[threadId] = System.nanoTime();
    }
    public static void unlockOCCEndTime(int threadId) {
        Metrics.RemoteOCCRuntime.unlockEndTime[threadId] = System.nanoTime();
        Metrics.RemoteOCCRuntime.unlockTimeStatistics[threadId].addValue(Metrics.RemoteOCCRuntime.unlockEndTime[threadId] - Metrics.RemoteOCCRuntime.unlockStartTime[threadId]);
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
    public static void WriteWorkerDSMetrics(int tthread) {
        try {
            File file = new File(MetricsDirectory + "worker.txt");
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
            fileWriter.write("thread_id\t rdmaRecvTime (s)\t prepareTime (s)\t prepareCacheTime (s)\t rdmaRemoteOperationTime (s)\t setupDependenciesTime (s)\t executeTime (s)\t rmaTime (s)\t exploreTime (s)\t commitTime (s)\t finishTime (s)\t rdmaSendResultTime (s)\n");
            double totalRdmaRecvTime = 0;
            double totalPrepareTime = 0;
            double totalPrepareCacheTime = 0;
            double totalRdmaRemoteOperationTime = 0;
            double totalSetupDependenciesTime = 0;
            double totalExecuteTime = 0;
            double totalRmaTime = 0;
            double totalExploreTime = 0;
            double totalCommitTime = 0;
            double totalFinishTime = 0;
            double totalRdmaSendResultTime = 0;
            double totalRounds = 0;
            double avgRounds = 0;
            for (int i = 0; i < tthread; i++) {
                totalRdmaRecvTime = totalRdmaRecvTime + Metrics.WorkerRuntime.rdmaRecvTimeStatistics[i].getSum();
                totalPrepareTime = totalPrepareTime + Metrics.WorkerRuntime.prepareTimeStatistics[i].getSum();
                totalPrepareCacheTime = totalPrepareCacheTime + Metrics.DSRuntime.prepareCacheTimeStatistics[i].getSum();
                totalRdmaRemoteOperationTime = totalRdmaRemoteOperationTime + Metrics.DSRuntime.rdmaRemoteOperationTimeStatistics[i].getSum();
                totalSetupDependenciesTime = totalSetupDependenciesTime + Metrics.DSRuntime.setupDependenciesTimeStatistics[i].getSum();
                totalExecuteTime = totalExecuteTime + Metrics.DSRuntime.executeTimeStatistics[i].getSum();
                totalRmaTime = totalRmaTime + Metrics.DSRuntime.rdmaAccessTimeStatistics[i].getSum();
                totalExploreTime = totalExploreTime + Metrics.DSRuntime.totalExecutionTimeStatistics[i].getSum() - Metrics.DSRuntime.rdmaAccessTimeStatistics[i].getSum() - Metrics.DSRuntime.executeTimeStatistics[i].getSum();
                totalCommitTime = totalCommitTime + Metrics.DSRuntime.commitTimeStatistics[i].getSum();
                totalFinishTime = totalFinishTime + Metrics.WorkerRuntime.finishTimeStatistics[i].getSum();
                totalRdmaSendResultTime = totalRdmaSendResultTime + Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[i].getSum();
                fileWriter.write(i + "\t" + Metrics.WorkerRuntime.rdmaRecvTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.prepareTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.DSRuntime.prepareCacheTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.DSRuntime.rdmaRemoteOperationTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.DSRuntime.setupDependenciesTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.DSRuntime.executeTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.DSRuntime.rdmaAccessTimeStatistics[i].getSum() / 1E9 + "\t" + (Metrics.DSRuntime.totalExecutionTimeStatistics[i].getSum() - Metrics.DSRuntime.rdmaAccessTimeStatistics[i].getSum() - Metrics.DSRuntime.executeTimeStatistics[i].getSum()) / 1E9 + "\t" + Metrics.DSRuntime.commitTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.finishTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[i].getSum() / 1E9 + "\n");
            }
            fileWriter.write("Avg Time: " + "\t" + totalRdmaRecvTime / tthread / 1E9 + "\t" + totalPrepareTime / tthread / 1E9 + "\t" + totalPrepareCacheTime / tthread / 1E9 + "\t" + totalRdmaRemoteOperationTime / tthread / 1E9 + "\t" + totalSetupDependenciesTime / tthread / 1E9 + "\t" + totalExecuteTime / tthread / 1E9 + "\t" + totalRmaTime / tthread / 1E9 + "\t" + totalExploreTime / tthread / 1E9 + "\t" + totalCommitTime / tthread / 1E9 + "\t" + totalFinishTime / tthread / 1E9 + "\t" + totalRdmaSendResultTime / tthread / 1E9 + "\n");
            double totalWorkerTime = totalRdmaRecvTime + totalPrepareTime + totalPrepareCacheTime + totalRdmaRemoteOperationTime + totalSetupDependenciesTime + totalExecuteTime + totalFinishTime + totalRdmaSendResultTime;
            fileWriter.write("Breakdown in percentage: " + "\n");
            fileWriter.write("RdmaRecv (%)\t Prepare (%)\t PrepareCache (%)\t RdmaRemoteOperation (%)\t SetupDependencies (%)\t Execute (%)\t Finish (%)\t RdmaSendResult (%)\n");
            fileWriter.write(totalRdmaRecvTime / totalWorkerTime + "\t" + totalPrepareTime / totalWorkerTime + "\t" + totalPrepareCacheTime / totalWorkerTime + "\t" + totalRdmaRemoteOperationTime / totalWorkerTime + "\t" + totalSetupDependenciesTime / totalWorkerTime + "\t" + totalExecuteTime / totalWorkerTime + "\t" + totalFinishTime / totalWorkerTime + "\t" + totalRdmaSendResultTime / totalWorkerTime + "\n");
            fileWriter.write("Communication Rounds: " + "\n");
            for (int i = 0; i < tthread; i++) {
                totalRounds = totalRounds + Metrics.WorkerRuntime.rdmaRounds[i].getSum();
                fileWriter.write(i + "\t" + Metrics.WorkerRuntime.rdmaRounds[i].getSum() + "\n");
            }
            fileWriter.write("Total Rounds: " + totalRounds + "\n");
            fileWriter.write("Avg Rounds: " + totalRounds / tthread + "\n");
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteWorkerRLMetrics(int tthread) {
        try {
            File file = new File(MetricsDirectory + "worker.txt");
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
            fileWriter.write("thread_id\t rdmaRecvTime (s)\t prepareTime (s)\t lockTime (s)\t rmaTime (s)\t executeTime (s)\t commitTime (s)\t unlockTime (s)\t finishTime (s)\t rdmaSendResultTime (s)\n");
            double totalRdmaRecvTime = 0;
            double totalPrepareTime = 0;
            double totalPrepareCacheTime = 0;
            double totalLockTime = 0;
            double totalRmaTime = 0;
            double totalExecuteTime = 0;
            double totalCommitTime = 0;
            double totalUnlockTime = 0;
            double totalFinishTime = 0;
            double totalRdmaSendResultTime = 0;
            double totalRounds = 0;
            double avgRounds = 0;
            for (int i = 0; i < tthread; i++) {
                totalRdmaRecvTime = totalRdmaRecvTime + Metrics.WorkerRuntime.rdmaRecvTimeStatistics[i].getSum();
                totalPrepareTime = totalPrepareTime + Metrics.WorkerRuntime.prepareTimeStatistics[i].getSum();
                totalPrepareCacheTime = totalPrepareCacheTime + Metrics.DSRuntime.prepareCacheTimeStatistics[i].getSum();
                totalLockTime = totalLockTime + Metrics.RemoteLockRuntime.lockTimeStatistics[i].getSum();
                totalRmaTime = totalRmaTime + Metrics.RemoteLockRuntime.rmaAndExecutionTimeStatistics[i].getSum() - Metrics.RemoteLockRuntime.executeTimeStatistics[i].getSum();
                totalExecuteTime = totalExecuteTime + Metrics.RemoteLockRuntime.executeTimeStatistics[i].getSum();
                totalCommitTime = totalCommitTime + Metrics.RemoteLockRuntime.commitTimeStatistics[i].getSum();
                totalUnlockTime = totalUnlockTime + Metrics.RemoteLockRuntime.unlockTimeStatistics[i].getSum();
                totalFinishTime = totalFinishTime + Metrics.WorkerRuntime.finishTimeStatistics[i].getSum();
                totalRdmaSendResultTime = totalRdmaSendResultTime + Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[i].getSum();
                fileWriter.write(i + "\t" + Metrics.WorkerRuntime.rdmaRecvTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.prepareTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.RemoteLockRuntime.lockTimeStatistics[i].getSum() / 1E9 + "\t" + (Metrics.RemoteLockRuntime.rmaAndExecutionTimeStatistics[i].getSum() - Metrics.RemoteLockRuntime.executeTimeStatistics[i].getSum()) / 1E9 + "\t" + Metrics.RemoteLockRuntime.executeTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.RemoteLockRuntime.commitTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.RemoteLockRuntime.unlockTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.finishTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[i].getSum() / 1E9 + "\n");
            }
            fileWriter.write("Avg Time: " + "\t" + totalRdmaRecvTime / tthread / 1E9 + "\t" + totalPrepareTime / tthread / 1E9 + "\t" + totalLockTime / tthread / 1E9 + "\t" + totalRmaTime / tthread / 1E9 + "\t" + totalExecuteTime / tthread / 1E9 + "\t" + totalCommitTime / tthread / 1E9 + "\t" + totalUnlockTime / tthread / 1E9 + "\t" + totalFinishTime / tthread / 1E9 + "\t" + totalRdmaSendResultTime / tthread / 1E9 + "\n");
            double totalWorkerTime = totalRdmaRecvTime + totalPrepareTime + totalLockTime + totalRmaTime + totalExecuteTime + totalCommitTime + totalUnlockTime + totalFinishTime + totalRdmaSendResultTime;
            fileWriter.write("Breakdown in percentage: " + "\n");
            fileWriter.write("RdmaRecv (%)\t Prepare (%)\t Lock (%)\t Rma (%)\t Execute (%)\t Commit (%)\t Unlock (%)\t Finish (%)\t RdmaSendResult (%)\n");
            fileWriter.write(totalRdmaRecvTime / totalWorkerTime + "\t" + totalPrepareTime / totalWorkerTime + "\t" + totalLockTime / totalWorkerTime + "\t" + totalRmaTime / totalWorkerTime + "\t" + totalExecuteTime / totalWorkerTime + "\t" + totalCommitTime / totalWorkerTime + "\t" + totalUnlockTime / totalWorkerTime + "\t" + totalFinishTime / totalWorkerTime + "\t" + totalRdmaSendResultTime / totalWorkerTime + "\n");
            fileWriter.write("Communication Rounds: " + "\n");
            for (int i = 0; i < tthread; i++) {
                totalRounds = totalRounds + Metrics.WorkerRuntime.rdmaRounds[i].getSum();
                fileWriter.write(i + "\t" + Metrics.WorkerRuntime.rdmaRounds[i].getSum() + "\n");
            }
            fileWriter.write("Total Rounds: " + totalRounds + "\n");
            fileWriter.write("Avg Rounds: " + totalRounds / tthread + "\n");
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteWorkerOCCMetrics(int tthread) {
        try {
            File file = new File(MetricsDirectory + "worker.txt");
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
            fileWriter.write("thread_id\t rdmaRecvTime (s)\t prepareTime (s)\t rmaTime (s)\t executeTime (s)\t validateTime (s)\t commitTime (s)\t unlockTime (s)\t finishTime (s)\t rdmaSendResultTime (s)\n");
            double totalRdmaRecvTime = 0;
            double totalPrepareTime = 0;
            double totalPrepareCacheTime = 0;
            double totalRmaTime = 0;
            double totalExecuteTime = 0;
            double totalValidateTime = 0;
            double totalCommitTime = 0;
            double totalUnlockTime = 0;
            double totalFinishTime = 0;
            double totalRdmaSendResultTime = 0;
            double totalRounds = 0;
            double avgRounds = 0;
            for (int i = 0; i < tthread; i++) {
                totalRdmaRecvTime = totalRdmaRecvTime + Metrics.WorkerRuntime.rdmaRecvTimeStatistics[i].getSum();
                totalPrepareTime = totalPrepareTime + Metrics.WorkerRuntime.prepareTimeStatistics[i].getSum();
                totalPrepareCacheTime = totalPrepareCacheTime + Metrics.DSRuntime.prepareCacheTimeStatistics[i].getSum();
                totalRmaTime = totalRmaTime + Metrics.RemoteOCCRuntime.rmaAndExecutionTimeStatistics[i].getSum() - Metrics.RemoteOCCRuntime.executeTimeStatistics[i].getSum();
                totalExecuteTime = totalExecuteTime + Metrics.RemoteOCCRuntime.executeTimeStatistics[i].getSum();
                totalValidateTime = totalValidateTime + Metrics.RemoteOCCRuntime.validateTimeStatistics[i].getSum();
                totalCommitTime = totalCommitTime + Metrics.RemoteOCCRuntime.commitTimeStatistics[i].getSum();
                totalUnlockTime = totalUnlockTime + Metrics.RemoteOCCRuntime.unlockTimeStatistics[i].getSum();
                totalFinishTime = totalFinishTime + Metrics.WorkerRuntime.finishTimeStatistics[i].getSum();
                totalRdmaSendResultTime = totalRdmaSendResultTime + Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[i].getSum();
                fileWriter.write(i + "\t" + Metrics.WorkerRuntime.rdmaRecvTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.prepareTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.RemoteLockRuntime.lockTimeStatistics[i].getSum() / 1E9 + "\t" + (Metrics.RemoteLockRuntime.rmaAndExecutionTimeStatistics[i].getSum() - Metrics.RemoteLockRuntime.executeTimeStatistics[i].getSum()) / 1E9 + "\t" + Metrics.RemoteLockRuntime.executeTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.RemoteLockRuntime.commitTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.RemoteLockRuntime.unlockTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.finishTimeStatistics[i].getSum() / 1E9 + "\t" + Metrics.WorkerRuntime.rdmaSendResultTimeStatistics[i].getSum() / 1E9 + "\n");
            }
            fileWriter.write("Avg Time: " + "\t" + totalRdmaRecvTime / tthread / 1E9 + "\t" + totalPrepareTime / tthread / 1E9 + "\t" + totalRmaTime / tthread / 1E9 + "\t" + totalExecuteTime / tthread / 1E9 + "\t" + totalValidateTime / tthread / 1E9 + "\t" + totalCommitTime / tthread / 1E9 + "\t" + totalUnlockTime / tthread / 1E9 + "\t" + totalFinishTime / tthread / 1E9 + "\t" + totalRdmaSendResultTime / tthread / 1E9 + "\n");
            double totalWorkerTime = totalRdmaRecvTime + totalPrepareTime + totalRmaTime + totalExecuteTime + totalValidateTime + totalCommitTime + totalUnlockTime + totalFinishTime + totalRdmaSendResultTime;
            fileWriter.write("Breakdown in percentage: " + "\n");
            fileWriter.write("RdmaRecv (%)\t Prepare (%)\t Rma (%)\t Execute (%)\t Validate (%)\t Commit (%)\t Unlock (%)\t Finish (%)\t RdmaSendResult (%)\n");
            fileWriter.write(totalRdmaRecvTime / totalWorkerTime + "\t" + totalPrepareTime / totalWorkerTime + "\t" + totalRmaTime / totalWorkerTime + "\t" + totalExecuteTime / totalWorkerTime + "\t" + totalValidateTime / totalWorkerTime + "\t" + totalCommitTime / totalWorkerTime + "\t" + totalUnlockTime / totalWorkerTime + "\t" + totalFinishTime / totalWorkerTime + "\t" + totalRdmaSendResultTime / totalWorkerTime + "\n");
            fileWriter.write("Communication Rounds: " + "\n");
            for (int i = 0; i < tthread; i++) {
                totalRounds = totalRounds + Metrics.WorkerRuntime.rdmaRounds[i].getSum();
                fileWriter.write(i + "\t" + Metrics.WorkerRuntime.rdmaRounds[i].getSum() + "\n");
            }
            fileWriter.write("Total Rounds: " + totalRounds + "\n");
            fileWriter.write("Avg Rounds: " + totalRounds / tthread + "\n");
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
    public static void WORKER_METRICS_REPORT(int tthread, String scheduler) {
        switch (scheduler) {
            case "DScheduler":
                WriteWorkerDSMetrics(tthread);
                break;
            case "RLScheduler":
                WriteWorkerRLMetrics(tthread);
                break;
            case "OCCScheduler":
                WriteWorkerOCCMetrics(tthread);
                break;
            default:
                log.error("Please select correct scheduler!");
                break;
        }
    }
}
