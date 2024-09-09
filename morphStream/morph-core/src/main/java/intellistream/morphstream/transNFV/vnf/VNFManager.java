package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.transNFV.common.SyncData;
import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CyclicBarrier;

public class VNFManager implements Runnable {
    private int stateStartID = 0;
    private static HashMap<Integer, VNFInstance> instanceMap = new HashMap<>();
    private static HashMap<Integer, Thread> instanceThreadMap = new HashMap<>();
    private static HashMap<Integer, LocalSVCCStateManager> instanceStateManagerMap = new HashMap<>();

    private final int instancePatternPunctuation = MorphStreamEnv.get().configuration().getInt("instancePatternPunctuation");
    private static final int pattern = 4;
    private static final String inputWorkloadPath = MorphStreamEnv.get().configuration().getString("inputWorkloadPath");
    private static final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private static final String nfvExperimentPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");

    private static final String expID = MorphStreamEnv.get().configuration().getString("experimentID");
    private static final String vnfID = MorphStreamEnv.get().configuration().getString("vnfID");
    private static final int numPackets = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private static final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
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

    private static int partitionGap = numItems / numInstances;
    private int totalRequestCounter = 0;
    private long overallStartTime = Long.MAX_VALUE;
    private long overallEndTime = Long.MIN_VALUE;
    private static double totalParseTimeMS = 0;
    private static double totalSyncTimeMS = 0;
    private static double totalUsefulTimeMS = 0;
    private static double totalCCSwitchTimeMS = 0;
    private static double totalTimeMS = 0;
    private static HashMap<Integer, ConcurrentLinkedDeque<VNFRequest>> latencyMap = new HashMap<>(); //instanceID -> instance's latency list
    private static SynchronizedDescriptiveStatistics instanceLatencyStats = new SynchronizedDescriptiveStatistics();
    private static HashMap<Integer, Double> puncThroughputMap = new HashMap<>(); //punctuationID -> punctuation overall throughput
    private static HashMap<Integer, Double> throughputMap = new HashMap<>(); //instanceID -> instance's throughput
    private static HashMap<Integer, Double> minLatencyMap = new HashMap<>(); //instanceID -> instance's min latency
    private static HashMap<Integer, Double> maxLatencyMap = new HashMap<>(); //instanceID -> instance's max latency
    private static HashMap<Integer, Double> avgLatencyMap = new HashMap<>(); //instanceID -> instance's avg latency
    private static HashMap<Integer, Double> percentile95Map = new HashMap<>(); //instanceID -> instance's 95th percentile latency

    public VNFManager() {
        initInstances();
    }

    private void initInstances() {
        CyclicBarrier finishBarrier = new CyclicBarrier(numInstances);
        for (int i = 0; i < numInstances; i++) {
            String csvFilePath;
            csvFilePath = String.format(inputWorkloadPath + "/instance_%d.csv", i);
            int instanceExpRequestCount = countLinesInCSV(csvFilePath);
            LocalSVCCStateManager localSVCCStateManager = new LocalSVCCStateManager(i);
            instanceStateManagerMap.put(i, localSVCCStateManager);
            VNFInstance instance = new VNFInstance(i,
                    stateStartID + i * partitionGap, stateStartID + (i + 1) * partitionGap, numItems,
                    ccStrategy, numTPGThreads, csvFilePath, localSVCCStateManager, finishBarrier, instanceExpRequestCount);
            Thread senderThread = new Thread(instance);
            instanceMap.put(i, instance);
            instanceThreadMap.put(i, senderThread);
        }
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

    /** General entry for instances to sync local state update to others */
    public static void broadcastUpdate(int originInstanceID, int tupleID, int value) {
        for (int i = 0; i < numInstances; i++) {
            if (i != originInstanceID) {
                instanceMap.get(i).addStateSync(new SyncData(tupleID, value));
            }
        }
    }

    @Override
    public void run() {
        startVNFInstances();
        double overallThroughput = joinVNFInstances();

        System.out.println("All VNF instances have completed processing.");
        System.out.println("Overall throughput: " + overallThroughput + " events/second");

        String outputFileDir = String.format(nfvExperimentPath + "/results/%s/vnfID=%s/numPackets=%d/numInstances=%d/" +
                        "numItems=%d/keySkew=%d/workloadSkew=%d/readRatio=%d/locality=%s/scopeRatio=%d/numTPGThreads=%d/" +
                        "numOffloadThreads=%d/puncInterval=%d/ccStrategy=%s/doMVCC=%d/udfComplexity=%d",
                expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, locality,
                scopeRatio, numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity);

        String patternString = toPatternString(pattern);
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        switch (experimentID) {
            case "5.1": // Preliminary study
                writeCSVThroughput(patternString, ccStrategy, overallThroughput);
                break;
            case "5.2.1": // Static VNF throughput and latency
                writeCSVThroughput(patternString, ccStrategy, overallThroughput);
                writeCSVLatency(patternString, ccStrategy);
                break;
            case "5.2.2": // Dynamic VNF throughput and latency
                computeDynamicThroughput();
                writeCSVDynamicThroughput(patternString, ccStrategy);
                writeCSVLatency(patternString, ccStrategy);
                break;
            case "5.3.1": // Dynamic VNF time breakdown
                computeTimeBreakdown();
                writeCSVBreakdown();
                break;
            case "5.4.1":
            case "5.4.2":
                writeCSVThroughput(outputFileDir, overallThroughput);
                break;
        }
    }

    public void startVNFInstances() {
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

        instanceLatencyStats.clear();
        for (int i = 0; i < numInstances; i++) {
            for (VNFRequest request : latencyMap.get(i)) {
                long latency = request.getFinishTime() - request.getCreateTime();
                double latencyInUS = latency / 1E3;
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

    private void computeDynamicThroughput() {
        int numPunc = numPackets / (instancePatternPunctuation * numInstances);
        for (int puncID = 1; puncID <= numPunc; puncID++) {
            long puncStartTime = Long.MAX_VALUE;
            long puncEndTime = Long.MIN_VALUE;
            for (int i = 0; i < numInstances; i++) {
                long[] instancePuncTimes = instanceMap.get(i).getPuncStartEndTimes(puncID);
                puncStartTime = Math.min(puncStartTime, instancePuncTimes[0]);
                puncEndTime = Math.max(puncEndTime, instancePuncTimes[1]);
            }
            double puncThroughput = instancePatternPunctuation * numInstances / ((puncEndTime - puncStartTime) / 1E9);
            puncThroughputMap.put(puncID, puncThroughput);
        }
    }


    private void computeTimeBreakdown() {
//        double aggInstanceParseTime = 0;
//        double aggInstanceSyncTime = 0;
//        double aggInstanceUsefulTime = 0;
//        double aggManagerSyncTime = 0;
//        double aggManagerUsefulTime = 0;
//        double aggCCSwitchTime = 0;
//        double aggTotalTime = 0;
//
//        /** Breakdown at instance level */
//        for (int i = 0; i < numInstances; i++) {
//            VNFInstance sender = senderMap.get(i);
//            aggInstanceParseTime = (double) sender.getAggParsingTime() / 1E6; // Millisecond
//            aggInstanceSyncTime = (double) sender.getAGG_SYNC_TIME() / 1E6;
//            aggInstanceUsefulTime = (double) sender.getAGG_USEFUL_TIME() / 1E6;
//            aggCCSwitchTime += (double) sender.getAggCCSwitchTime() / 1E6;
//            for (VNFRequest request : latencyMap.get(i)) {
//                aggTotalTime += (double) (request.getFinishTime() - request.getCreateTime()) / 1E6;
//            }
//        }
//
//        /** Breakdown at manager level */
//        if (ccStrategy == 0) { // Partitioning
//            aggManagerSyncTime = PartitionStateManager.getManagerEventSyncTime() / 1E6;
//            aggManagerUsefulTime = PartitionStateManager.getManagerEventUsefulTime() / 1E6;
//            // Caching time breakdown is done at instance level
//        } else if (ccStrategy == 2) { // Offloading
//            aggManagerUsefulTime = OffloadCCExecutorService.getAggUsefulTime().get() / 1E6;
//            aggInstanceSyncTime -= aggManagerUsefulTime;
//        } else if (ccStrategy == 3) {
//            //TODO: Get time breakdown from TPG threads
//        } else if (ccStrategy == 4) { // OpenNF
//            aggManagerUsefulTime = OpenNFStateManager.getAggUsefulTime() / 1E6;
//            aggInstanceSyncTime -= aggManagerUsefulTime;
//        } else if (ccStrategy == 5) { // CHC
//            aggManagerUsefulTime = CHCStateManager.getAggUsefulTime() / 1E6;
//            aggInstanceSyncTime -= aggManagerUsefulTime;
//        } else if (ccStrategy == 7) {
//            aggManagerSyncTime += PartitionStateManager.getManagerEventSyncTime() / 1E6;
//            aggManagerSyncTime += OffloadCCExecutorService.getAggSyncTime().get() / 1E6;
//            //TODO: Get time breakdown from TPG threads
//            aggManagerUsefulTime += PartitionStateManager.getManagerEventUsefulTime() / 1E6;
//            aggManagerUsefulTime += OffloadCCExecutorService.getAggUsefulTime().get() / 1E6;
//            //TODO: Get time breakdown from TPG threads
//        }
//
//        totalParseTimeMS = aggInstanceParseTime;
//        totalSyncTimeMS = aggInstanceSyncTime + aggManagerSyncTime;
//        totalUsefulTimeMS = aggInstanceUsefulTime + aggManagerUsefulTime;
//        totalCCSwitchTimeMS = aggCCSwitchTime;
//        totalTimeMS = aggTotalTime;
    }

    private static void writeCSVDynamicThroughput(String pattern, String ccStrategy) {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "throughput");
        String directoryPath = String.format("%s/%s", baseDirectory, pattern);

        // Create the directory if it does not exist
        File dir = new File(directoryPath);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory: " + directoryPath);
                return;
            }
        }

        // Ensure the subdirectory for the strategy exists
        String strategyDirectoryPath = String.format("%s/%s", directoryPath, ccStrategy);
        File strategyDir = new File(strategyDirectoryPath);
        if (!strategyDir.exists()) {
            if (!strategyDir.mkdirs()) {
                System.out.println("Failed to create the strategy directory: " + strategyDirectoryPath);
                return;
            }
        }

        for (int puncID : puncThroughputMap.keySet()) {
            String filePath = String.format("%s/punc_%d.csv", strategyDirectoryPath, puncID);
            System.out.println("Writing to " + filePath);
            File file = new File(filePath);
            if (file.exists()) {
                boolean isDeleted = file.delete();
                if (!isDeleted) {
                    System.out.println("Failed to delete existing file: " + filePath);
                    return;
                }
            }
            try (FileWriter fileWriter = new FileWriter(file)) {
                String lineToWrite = pattern + "," + ccStrategy + "," + puncThroughputMap.get(puncID) + "\n";
                fileWriter.write(lineToWrite);
                System.out.println("Data written to CSV file successfully: " + filePath);
            } catch (IOException e) {
                System.out.println("An error occurred while writing to the CSV file: " + filePath);
                e.printStackTrace();
            }
        }
    }

    private static void writeCSVLatency(String pattern, String ccStrategy) {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "latency");
        String directoryPath = String.format("%s/%s", baseDirectory, pattern);
        String filePath = String.format("%s/%s.csv", directoryPath, ccStrategy);
        System.out.println("Writing to " + filePath);

        File dir = new File(directoryPath);
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
                String lineToWrite = String.valueOf(latency) + "\n";
                fileWriter.write(lineToWrite);
            }
            System.out.println("Data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
        }
    }

    private static void writeCSVThroughput(String pattern, String ccStrategy, double throughput) {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "throughput");
        String directoryPath = String.format("%s/%s", baseDirectory, pattern);
        String filePath = String.format("%s/%s.csv", directoryPath, ccStrategy);
        System.out.println("Writing to " + filePath);

        File dir = new File(directoryPath);
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
            String lineToWrite = pattern + "," + ccStrategy + "," + throughput + "\n";
            fileWriter.write(lineToWrite);
            System.out.println("Data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
        }
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
            String lineToWrite = pattern + "," + ccStrategy + "," + throughput + "\n";
            fileWriter.write(lineToWrite);
            System.out.println("Data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
        }
    }

    private static void writeCSVScalability(String pattern, String ccStrategy, double throughput) {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "throughput");
        String directoryPath = String.format("%s/numInstance_%d/%s", baseDirectory, numInstances, pattern);
        String filePath = String.format("%s/%s.csv", directoryPath, ccStrategy);
        System.out.println("Writing to " + filePath);
        File dir = new File(directoryPath);
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
            String lineToWrite = pattern + "," + ccStrategy + "," + throughput + "\n";
            fileWriter.write(lineToWrite);
            System.out.println("Data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
        }
    }

    private static void writeCSVBreakdown() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "breakdown");
        String directoryPath = String.format("%s/numInstance_%d/%s", baseDirectory, numInstances, toPatternString(pattern));
        String filePath = String.format("%s/%s.csv", directoryPath, ccStrategy);
        System.out.println("Writing to " + filePath);
        File dir = new File(directoryPath);
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
            String lineToWrite = totalParseTimeMS + "," + totalSyncTimeMS + "," + totalUsefulTimeMS + "," + totalCCSwitchTimeMS + "\n";
            fileWriter.write(lineToWrite);
            System.out.println("Data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
        }
    }

    private static void writeIndicatorFile(String fileName) {
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String directoryPath = rootPath + "/indicators";
        String filePath = String.format("%s/%s.csv", directoryPath, fileName);
        System.out.println("Writing indicator: " + fileName);

        File dir = new File(directoryPath);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return; // Stop further processing if unable to create the directory
            }
        }

        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }

        try {
            file.createNewFile();
        } catch (IOException e) {
            System.out.println("An error occurred while creating the file.");
            e.printStackTrace();
        }
    }

    private static String toPatternString(int pattern) {
        switch (pattern) {
            case 0:
                return "loneOperative";
            case 1:
                return "sharedReaders";
            case 2:
                return "sharedWriters";
            case 3:
                return "mutualInteractive";
            case 4:
                return "dynamic";
            default:
                return "invalid";
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

}
