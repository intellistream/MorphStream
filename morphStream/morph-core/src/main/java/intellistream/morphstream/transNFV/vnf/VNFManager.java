package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.executors.LocalExecutor;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

public class VNFManager implements Runnable {
    private int stateStartID = 0;
    private static HashMap<Integer, VNFInstance> instanceMap = new HashMap<>();
    private static HashMap<Integer, Thread> instanceThreadMap = new HashMap<>();
    private static HashMap<Integer, LocalSVCCStateManager> instanceStateManagerMap = new HashMap<>();
    private static HashMap<Integer, LocalExecutor> localExecutorMap = new HashMap<>();
    private final HashMap<Integer, Thread> localExecutorThreadMap = new HashMap<>();
    private static HashMap<Integer, BlockingQueue<VNFRequest>> localExecutorInputQueueMap = new HashMap<>();
    private static HashMap<Integer, List<Integer>> instanceToLocalExecutorsMap = new HashMap<>();

    private static final String inputWorkloadPath = MorphStreamEnv.get().configuration().getString("inputWorkloadPath");
    private static final String inputMonitorWorkloadPath = MorphStreamEnv.get().configuration().getString("inputMonitorWorkloadPath");
    private static final String nfvExperimentPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");

    private static final String expID = MorphStreamEnv.get().configuration().getString("experimentID");
    private static final String vnfID = MorphStreamEnv.get().configuration().getString("vnfID");
    private static final int numPackets = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private static final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private static final int numLocalThreads = MorphStreamEnv.get().configuration().getInt("numLocalThreads");
    private static final int numItems = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");

    private static final int keySkew = MorphStreamEnv.get().configuration().getInt("keySkew");
    private static final int workloadSkew = MorphStreamEnv.get().configuration().getInt("workloadSkew");
    private static final int readRatio = MorphStreamEnv.get().configuration().getInt("readRatio");
    private static final int locality = MorphStreamEnv.get().configuration().getInt("locality");
    private static final int scopeRatio = MorphStreamEnv.get().configuration().getInt("scopeRatio");

    private static final int numTPGThreads = MorphStreamEnv.get().configuration().getInt("tthread");
    private static final int numOffloadThreads = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
    private static final int puncInterval = MorphStreamEnv.get().configuration().getInt("checkpoint");
    private static final String ccStrategy = MorphStreamEnv.get().configuration().getString("ccStrategy");
    private static final int doMVCC = MorphStreamEnv.get().configuration().getInt("doMVCC");
    private static final int udfComplexity = MorphStreamEnv.get().configuration().getInt("udfComplexity");
    private static final int workloadInterval = MorphStreamEnv.get().configuration().getInt("workloadInterval");
    private static final int monitorWindowSize = MorphStreamEnv.get().configuration().getInt("monitorWindowSize");
    private static final int hardcodeSwitch = MorphStreamEnv.get().configuration().getInt("hardcodeSwitch");

    private static int partitionGap = numItems / numInstances;
    private int totalRequestCounter = 0;
    private long overallStartTime = Long.MAX_VALUE;
    private long overallEndTime = Long.MIN_VALUE;
    private static final HashMap<Integer, Integer> instancePartitionStartMap = new HashMap<>();
    private static final HashMap<Integer, Integer> instancePartitionEndMap = new HashMap<>();
    private static HashMap<Integer, ConcurrentLinkedDeque<VNFRequest>> latencyMap = new HashMap<>(); //instanceID -> instance's latency list
    private static SynchronizedDescriptiveStatistics instanceLatencyStats = new SynchronizedDescriptiveStatistics();

    private static long initEndNanoTimestamp = -1;
    private static long processEndNanoTimestamp = -1;

    private static double totalParseTimeMS = 0;
    private static double totalSyncTimeMS = 0;
    private static double totalUsefulTimeMS = 0;
    private static double totalSwitchTimeMS = 0;
    private static double totalTimeMS = 0;

    public VNFManager() {
        initInstances();
    }

    private void initInstances() {
        String universalInputPath;
        if (expID.equals("5.6.2")) {
            universalInputPath = inputMonitorWorkloadPath;
        } else {
            universalInputPath = inputWorkloadPath;
        }

        CyclicBarrier finishBarrier = new CyclicBarrier(numInstances);
        for (int i = 0; i < numInstances; i++) {
            String csvFilePath;
            csvFilePath = String.format(universalInputPath + "/instance_%d.csv", i);
            int instanceExpRequestCount = countLinesInCSV(csvFilePath);
            LocalSVCCStateManager localSVCCStateManager = new LocalSVCCStateManager(i);
            instanceStateManagerMap.put(i, localSVCCStateManager);
            int partitionStart = stateStartID + i * partitionGap;
            int partitionEnd = stateStartID + (i + 1) * partitionGap;
            VNFInstance instance = new VNFInstance(i,
                    partitionStart, partitionEnd, numItems,
                    ccStrategy, numTPGThreads, csvFilePath, localSVCCStateManager, finishBarrier, instanceExpRequestCount);
            Thread senderThread = new Thread(instance);
            instanceMap.put(i, instance);
            instanceThreadMap.put(i, senderThread);
            instancePartitionStartMap.put(i, partitionStart);
            instancePartitionEndMap.put(i, partitionEnd);
            instanceToLocalExecutorsMap.put(i, new ArrayList<>());
        }

        for (int i = 0; i < numLocalThreads; i++) {
            /** Prepare local executors for Partitioning, Replication, OpenNF, CHC and S6 */
            BlockingQueue<VNFRequest> localExecutorInputQueue = new LinkedBlockingQueue<>();
            localExecutorInputQueueMap.put(i, localExecutorInputQueue);

            LocalExecutor localExecutor = new LocalExecutor(i, localExecutorInputQueue,
                    instancePartitionStartMap, instancePartitionEndMap);
            localExecutorMap.put(i, localExecutor);

            Thread localExecutorThread = new Thread(localExecutor);
            localExecutorThreadMap.put(i, localExecutorThread);
        }

//        for (int i=0; i<numLocalThreads; i++) {
//            int instanceID = i % numInstances;
//            instanceToLocalExecutorsMap.get(instanceID).add(i);
//        }

        for (int i=0; i<numInstances; i++) {
            int localExecutorID = i % numLocalThreads;
            instanceToLocalExecutorsMap.get(i).add(localExecutorID); //TODO: Assume numLocalThreads always less than numInstances
        }
//        System.out.println("All instances and local executors have been initialized.");

    }

    public static VNFInstance getInstance(int id) {
        return instanceMap.get(id);
    }
    public static HashMap<Integer, VNFInstance> getAllInstances() {
        return instanceMap;
    }
    public static LocalSVCCStateManager getInstanceStateManager(int id) {
        return instanceStateManagerMap.get(id);
    }
    public static int getPartitionedInstanceID(int tupleID) {
        return tupleID / partitionGap;
    }
    public static void submitLocalExecutorRequest(VNFRequest request) {
        int instanceID = request.getInstanceID();
        //TODO: Implement iterative request submission when numLocalExecutor > numInstances
        int localExecutorID = instanceToLocalExecutorsMap.get(instanceID).get(0);
        localExecutorInputQueueMap.get(localExecutorID).offer(request);
    }
    public static void stopLocalExecutors() {
        VNFRequest stopSignal = new VNFRequest(-1, -1, -1, -1, "-1", "-1", -1, -1, -1, -1);
        for (int i = 0; i < numLocalThreads; i++) {
            localExecutorInputQueueMap.get(i).offer(stopSignal);
        }
    }

    @Override
    public void run() {
        startVNFInstances();
        double overallThroughput = joinVNFInstances();

        System.out.println("All VNF instances have completed processing.");
        System.out.println("Overall throughput: " + overallThroughput + " events/second");
        String outputFileDir = getOutputFileDirectory();
        String outputFileDirMonitor = getOutputFileDirectoryMonitor();

        switch (expID) {
            case "5.1": // Dynamic workload, throughput, three existing strategies
                writeCSVThroughput(outputFileDir, overallThroughput); //TODO: To be aligned
                break;
            case "5.2.1": // Static workload, throughput and latency
                writeCSVThroughput(outputFileDir, overallThroughput); //TODO: To be aligned
                break;
            case "5.2.2_phase1": // Dynamic workload, throughput and latency
            case "5.2.2_phase2":
            case "5.2.2_phase3":
            case "5.2.2_phase4":
            case "5.2.3":
                writeCSVThroughput(outputFileDir, overallThroughput);
                writeCSVLatency(outputFileDir);
                break;
            case "5.3_phase1": // Dynamic workload, Time breakdown
            case "5.3_phase2":
            case "5.3_phase3":
            case "5.3_phase4":
                computeTimeBreakdown();
                writeCSVBreakdown(outputFileDir);
                break;
            case "5.4.1": // Static workload, throughput and latency
            case "5.4.2":
            case "5.4.3":
                writeCSVThroughput(outputFileDir, overallThroughput);
                writeCSVLatency(outputFileDir);
                break;
            case "5.5":
            case "5.5_Evaluation":
            case "5.5_Inference":
                writeCSVThroughput(outputFileDir, overallThroughput);
                break;
            case "5.6.1": // Dynamic workload, throughput
                writeCSVThroughput(outputFileDir, overallThroughput);
                break;
            case "5.6.2":
                writeCSVThroughputMonitor(outputFileDirMonitor, overallThroughput);
                computeTimeBreakdown();
                writeCSVBreakdown(outputFileDirMonitor);
                break;
        }
    }

    public void startVNFInstances() {
        if (! (ccStrategy.equals("Proactive") || ccStrategy.equals("Offloading")) ) {
            for (int i = 0; i < numLocalThreads; i++) {
                localExecutorThreadMap.get(i).start();
            }
        }

        for (int i = 0; i < numInstances; i++) {
            instanceThreadMap.get(i).start();
        }
    }

    public double joinVNFInstances() {
        for (int i = 0; i < numInstances; i++) {
            try {
                instanceThreadMap.get(i).join();
                totalRequestCounter += instanceMap.get(i).getFinishedRequestCount();
                overallStartTime = Math.min(overallStartTime, instanceMap.get(i).getOverallStartTime());
                overallEndTime = Math.max(overallEndTime, instanceMap.get(i).getOverallEndTime());
                latencyMap.put(i, instanceMap.get(i).getFinishedReqStorage());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (! (ccStrategy.equals("Proactive") || ccStrategy.equals("Offloading")) ) { //TODO: Messy thread management, to be optimized
            try {
                for (int i = 0; i < numLocalThreads; i++) {
                    localExecutorThreadMap.get(i).join();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        instanceLatencyStats.clear();
        for (int i = 0; i < numInstances; i++) {
            for (VNFRequest request : latencyMap.get(i)) {
                long latency = request.getFinishTime() - request.getCreateTime(); // in nanoseconds (1e-9)
                double latencyInUS = latency / 1E3; // in microseconds (1e-6)
                instanceLatencyStats.addValue(latencyInUS);
            }
        }

        double minLatency = instanceLatencyStats.getMin();
        double maxLatency = instanceLatencyStats.getMax();
        double avgLatency = instanceLatencyStats.getMean();
        double percentile95 = instanceLatencyStats.getPercentile(95);

        System.out.println(ccStrategy + " min latency (1e-6): " + minLatency);
        System.out.println(ccStrategy + " max latency (1e-6): " + maxLatency);
        System.out.println(ccStrategy + " avg latency (1e-6): " + avgLatency);
        System.out.println(ccStrategy + " 95 latency (1e-6): " + percentile95);

        long size = instanceLatencyStats.getN();
        long overallDuration = overallEndTime - overallStartTime;
        double overallThroughput = totalRequestCounter / (overallDuration / 1E9);
        return overallThroughput;
    }


    private void computeTimeBreakdown() {
        double aggInstanceParseTime = 0;
        double aggInstanceSyncTime = 0;
        double aggInstanceUsefulTime = 0;

        double aggLocalExecutorParseTime = 0;
        double aggLocalExecutorSyncTime = 0;
        double aggLocalExecutorUsefulTime = 0;

        double aggManagerParseTime = 0;
        double aggManagerSyncTime = 0;
        double aggManagerUsefulTime = 0;
        double aggCCSwitchTime = 0;
        double aggMetaDataTime = 0;
        double aggTotalTime = 0;

        /** Breakdown at instance level */
        for (int i = 0; i < numInstances; i++) {
            VNFInstance instance = instanceMap.get(i);
            aggInstanceParseTime += (double) instance.getAggParsingTime() / 1E6;
            aggInstanceSyncTime += (double) instance.getAGG_SYNC_TIME() / 1E6;
            aggInstanceUsefulTime += (double) instance.getAGG_USEFUL_TIME() / 1E6;
            aggCCSwitchTime += (double) instance.getAGG_CC_SWITCH_TIME() / 1E6;
            aggMetaDataTime += (double) instance.getAGG_METADATA_TIME() / 1E6;
//            for (VNFRequest request : latencyMap.get(i)) {
//                aggTotalTime += (double) (request.getFinishTime() - request.getCreateTime()) / 1E6;
//            }
        }

        for (int i = 0; i < numLocalThreads; i++) {
            LocalExecutor localExecutor = localExecutorMap.get(i);
            aggLocalExecutorParseTime += (double) localExecutor.getAGG_PARSING_TIME() / 1E6;
            aggLocalExecutorSyncTime += (double) localExecutor.getAGG_SYNC_TIME() / 1E6;
            aggLocalExecutorUsefulTime += (double) localExecutor.getAGG_USEFUL_TIME() / 1E6;
        }

        aggInstanceParseTime /= numInstances;
        aggInstanceSyncTime /= numInstances;
        aggInstanceUsefulTime /= numInstances;
        aggCCSwitchTime /= numInstances;
        aggMetaDataTime /= numInstances;

        aggLocalExecutorParseTime /= numInstances;
        aggLocalExecutorSyncTime /= numInstances;
        aggLocalExecutorUsefulTime /= numInstances;

        /** Breakdown at manager level */
        if (Objects.equals(ccStrategy, "Partitioning")) {
            aggManagerParseTime = MorphStreamEnv.get().getTransNFVStateManager().getPartitionStateManager().getAGG_PARSING_TIME() / 1E6;
            aggManagerUsefulTime = MorphStreamEnv.get().getTransNFVStateManager().getPartitionStateManager().getAGG_USEFUL_TIME() / 1E6;
        } else if (Objects.equals(ccStrategy, "Replication")) {
            aggManagerParseTime = MorphStreamEnv.get().getTransNFVStateManager().getReplicationStateManager().getAGG_PARSING_TIME() / 1E6;
        } else if (Objects.equals(ccStrategy, "Offloading")) {
            aggManagerParseTime = MorphStreamEnv.get().getTransNFVStateManager().getOffloadAvgAggParsingTime() / 1E6;
            aggManagerUsefulTime = MorphStreamEnv.get().getTransNFVStateManager().getOffloadAvgAggUsefulTime() / 1E6;
        } else if (Objects.equals(ccStrategy, "Proactive")) {
            //TODO: Get time breakdown from TPG threads
        } else if (Objects.equals(ccStrategy, "OpenNF")) {
            aggManagerParseTime = MorphStreamEnv.get().getTransNFVStateManager().getOpenNFStateManager().getAGG_PARSING_TIME() / 1E6;
            aggManagerUsefulTime = MorphStreamEnv.get().getTransNFVStateManager().getOpenNFStateManager().getAGG_USEFUL_TIME() / 1E6;
        } else if (Objects.equals(ccStrategy, "CHC")) {
            aggManagerParseTime = MorphStreamEnv.get().getTransNFVStateManager().getCHCStateManager().getAGG_PARSING_TIME() / 1E6;
            aggManagerUsefulTime = MorphStreamEnv.get().getTransNFVStateManager().getCHCStateManager().getAGG_USEFUL_TIME() / 1E6;
        } else if (Objects.equals(ccStrategy, "S6")) {
            aggManagerParseTime = MorphStreamEnv.get().getTransNFVStateManager().getS6StateManager().getAGG_PARSING_TIME() / 1E6;
        } else if (Objects.equals(ccStrategy, "Adaptive")) {
            //TODO: Get time breakdown from AdaptiveCC
        }

        //TODO: Double-check the time breakdown analysis
        totalParseTimeMS = aggManagerParseTime + aggInstanceParseTime + aggLocalExecutorParseTime;
        totalUsefulTimeMS = aggManagerUsefulTime + aggInstanceUsefulTime + aggLocalExecutorUsefulTime;
        totalSyncTimeMS = aggInstanceSyncTime - aggManagerParseTime - aggManagerUsefulTime - aggLocalExecutorParseTime - aggLocalExecutorUsefulTime;
        totalSwitchTimeMS = aggCCSwitchTime + aggMetaDataTime;
    }

    private static void writeCSVThroughput(String outputDir, double throughput) {
        String filePath = String.format("%s/throughput.csv", outputDir);
        System.out.println("Writing to " + filePath);
        File dir = new File(outputDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return;
            }
        }
        File file = new File(filePath);
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                System.out.println("Failed to delete existing file.");
                return;
            }
        }
        try (FileWriter fileWriter = new FileWriter(file)) {
            String lineToWrite = expID + "," + ccStrategy + "," + throughput + "\n";
            fileWriter.write(lineToWrite);
            System.out.println("Throughput data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            throw new RuntimeException(e);
        }
    }

    private static void writeCSVThroughputMonitor(String outputDir, double throughput) {
        String filePath = String.format("%s/throughput.csv", outputDir);
        System.out.println("Writing to " + filePath);
        File dir = new File(outputDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return;
            }
        }
        File file = new File(filePath);
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                System.out.println("Failed to delete existing file.");
                return;
            }
        }
        try (FileWriter fileWriter = new FileWriter(file)) {
            String lineToWrite = expID + "," + ccStrategy + "," + throughput + "\n";
            fileWriter.write(lineToWrite);
            System.out.println("Throughput data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            throw new RuntimeException(e);
        }
    }

    private static void writeCSVLatency(String outputDir) {
        String filePath = String.format("%s/latency.csv", outputDir);
        System.out.println("Writing to " + filePath);
        File dir = new File(outputDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return;
            }
        }
        File file = new File(filePath);
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                System.out.println("Failed to delete existing file.");
                return;
            }
        }
        try (FileWriter fileWriter = new FileWriter(file)) {
            for (double latency : instanceLatencyStats.getValues()) {
                String lineToWrite = latency + "\n";
                fileWriter.write(lineToWrite);
            }
            System.out.println("Latency data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            throw new RuntimeException(e);
        }
    }


    private static void writeCSVBreakdown(String outputDir) {
        String filePath = String.format("%s/breakdown.csv", outputDir);
        System.out.println("Writing to " + filePath);
        File dir = new File(outputDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return;
            }
        }
        File file = new File(filePath);
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                System.out.println("Failed to delete existing file.");
                return;
            }
        }
        try (FileWriter fileWriter = new FileWriter(file)) {
//            String lineToWrite = totalParseTimeMS + "," + totalSyncTimeMS + "," + totalUsefulTimeMS + "," + totalCCSwitchTimeMS + "," + totalTimeMS + "\n";
            String lineToWrite = totalParseTimeMS + "," + totalSyncTimeMS + "," + totalUsefulTimeMS + "," + totalSwitchTimeMS + "\n";
            fileWriter.write(lineToWrite);
            System.out.println("Time breakdown data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            throw new RuntimeException(e);
        }
    }

    public static int countLinesInCSV(String filePath) {
        int lineCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while (br.readLine() != null) {
                lineCount++;
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
        }
        return lineCount;
    }

    public static String getOutputFileDirectory() {
        return String.format(nfvExperimentPath + "/results/%s/vnfID=%s/numPackets=%d/numInstances=%d/" +
                        "numItems=%d/keySkew=%d/workloadSkew=%d/readRatio=%d/locality=%s/scopeRatio=%d/numTPGThreads=%d/" +
                        "numOffloadThreads=%d/puncInterval=%d/ccStrategy=%s/doMVCC=%d/udfComplexity=%d",
                expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, locality,
                scopeRatio, numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity);
    }

    public static String getOutputFileDirectoryMonitor() {
        return String.format(nfvExperimentPath + "/results/%s/vnfID=%s/numPackets=%d/numInstances=%d/" +
                        "numItems=%d/keySkew=%d/workloadSkew=%d/readRatio=%d/locality=%s/scopeRatio=%d/numTPGThreads=%d/" +
                        "numOffloadThreads=%d/puncInterval=%d/ccStrategy=%s/doMVCC=%d/udfComplexity=%d/workloadInterval=%d/monitorWindowSize=%d/hardcodeSwitch=%d",
                expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, locality,
                scopeRatio, numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, workloadInterval, monitorWindowSize, hardcodeSwitch);
    }

}
