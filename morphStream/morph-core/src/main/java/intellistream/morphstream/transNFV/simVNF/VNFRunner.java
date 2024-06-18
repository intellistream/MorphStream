package intellistream.morphstream.transNFV.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.CHCController;
import intellistream.morphstream.transNFV.OffloadCCThread;
import intellistream.morphstream.transNFV.OpenNFController;
import intellistream.morphstream.transNFV.PartitionCCThread;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CyclicBarrier;

public class VNFRunner implements Runnable {
    private int stateStartID = 0;
    private String patternString;
    private static HashMap<Integer, VNFInstance> senderMap = new HashMap<>();
    private static HashMap<Integer, Thread> senderThreadMap = new HashMap<>();
    private static HashMap<Integer, ConcurrentLinkedDeque<VNFRequest>> latencyMap = new HashMap<>(); //instanceID -> instance's latency list
    private static SynchronizedDescriptiveStatistics instanceLatencyStats = new SynchronizedDescriptiveStatistics();
    private static HashMap<Integer, Double> puncThroughputMap = new HashMap<>(); //punctuationID -> punctuation overall throughput
    private static HashMap<Integer, Double> throughputMap = new HashMap<>(); //instanceID -> instance's throughput
    private static HashMap<Integer, Double> minLatencyMap = new HashMap<>(); //instanceID -> instance's min latency
    private static HashMap<Integer, Double> maxLatencyMap = new HashMap<>(); //instanceID -> instance's max latency
    private static HashMap<Integer, Double> avgLatencyMap = new HashMap<>(); //instanceID -> instance's avg latency
    private static HashMap<Integer, Double> percentile95Map = new HashMap<>(); //instanceID -> instance's 95th percentile latency
    private int totalRequestCounter = 0;
    private long overallStartTime = Long.MAX_VALUE;
    private long overallEndTime = Long.MIN_VALUE;
    private final int totalRequests = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private final int instancePatternPunctuation = MorphStreamEnv.get().configuration().getInt("instancePatternPunctuation");
    private static final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final int stateRange = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private static final int ccStrategy = MorphStreamEnv.get().configuration().getInt("ccStrategy");
    private static final int pattern = MorphStreamEnv.get().configuration().getInt("workloadPattern");
    private static final int numTPGThreads = MorphStreamEnv.get().configuration().getInt("tthread");
    private static final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private static double totalParseTimeMS = 0;
    private static double totalSyncTimeMS = 0;
    private static double totalUsefulTimeMS = 0;
    private static double totalCCSwitchTimeMS = 0;
    private static double totalTimeMS = 0;

    public VNFRunner() {
        this.patternString = toPatternString(pattern);
        CyclicBarrier finishBarrier = new CyclicBarrier(vnfInstanceNum);
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String vnfID = MorphStreamEnv.get().configuration().getString("vnfID");

        for (int i = 0; i < vnfInstanceNum; i++) {
            String csvFilePath;
            if (experimentID == "5.1") {
                csvFilePath = String.format(rootPath + "/pattern_files/%s/instanceNum_%d/%s/instance_%d.csv", experimentID, vnfInstanceNum, vnfID, i);
            } else {
                csvFilePath = String.format(rootPath + "/pattern_files/%s/instanceNum_%d/%s/instance_%d.csv", experimentID, vnfInstanceNum, patternString, i);
            }
            int stateGap = stateRange / vnfInstanceNum;
            int instanceExpRequestCount = totalRequests / vnfInstanceNum;
            VNFInstance sender = new VNFInstance(i,
                    stateStartID + i * stateGap, stateStartID + (i + 1) * stateGap, stateRange,
                    ccStrategy, numTPGThreads, csvFilePath, finishBarrier, instanceExpRequestCount);
            Thread senderThread = new Thread(sender);
            senderMap.put(i, sender);
            senderThreadMap.put(i, senderThread);
        }
    }

    public static VNFInstance getSender(int id) {
        return senderMap.get(id);
    }
    public static HashMap<Integer, VNFInstance> getSenderMap() {
        return senderMap;
    }

    @Override
    public void run() {
        startVNFInstances();
        double overallThroughput = joinVNFInstances();

        System.out.println("All VNF instances have completed processing.");
        System.out.println("Overall throughput: " + overallThroughput + " events/second");
        String patternString = toPatternString(pattern);
        String ccStrategyString = toStringStrategy(ccStrategy);

        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        switch (experimentID) {
            case "5.1": // Preliminary study
                writeCSVThroughput(patternString, ccStrategyString, overallThroughput);
                break;
            case "5.2.1": // Static throughput and latency
                writeCSVThroughput(patternString, ccStrategyString, overallThroughput);
                writeCSVLatency(patternString, ccStrategyString);
                break;
            case "5.2.2": // Dynamic throughput
                computeDynamicThroughput();
                writeCSVDynamicThroughput(patternString, ccStrategyString);
                break;
            case "5.2.2_dynamic": // Dynamic throughput
                computeDynamicThroughput();
                writeCSVDynamicThroughput(patternString, ccStrategyString);
                break;
            case "5.3.1":
                computeTimeBreakdown();
                writeCSVBreakdown();
                break;
            case "5.4.3":
                writeCSVScalability(patternString, ccStrategyString, overallThroughput);
                break;
        }
    }

    public void startVNFInstances() {
        for (int i = 0; i < vnfInstanceNum; i++) {
            senderThreadMap.get(i).start();
        }
    }

    public double joinVNFInstances() {
        for (int i = 0; i < vnfInstanceNum; i++) {
            try {
                senderThreadMap.get(i).join();
                totalRequestCounter += senderMap.get(i).getFinishedRequestCount();
                overallStartTime = Math.min(overallStartTime, senderMap.get(i).getOverallStartTime());
                overallEndTime = Math.max(overallEndTime, senderMap.get(i).getOverallEndTime());
                latencyMap.put(i, senderMap.get(i).getFinishedReqStorage());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        instanceLatencyStats.clear();
        for (int i = 0; i < vnfInstanceNum; i++) {
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
        int numPunc = totalRequests / (instancePatternPunctuation * vnfInstanceNum);
        for (int puncID = 1; puncID <= numPunc; puncID++) {
            long puncStartTime = Long.MAX_VALUE;
            long puncEndTime = Long.MIN_VALUE;
            for (int i = 0; i < vnfInstanceNum; i++) {
                long[] instancePuncTimes = senderMap.get(i).getPuncStartEndTimes(puncID);
                puncStartTime = Math.min(puncStartTime, instancePuncTimes[0]);
                puncEndTime = Math.max(puncEndTime, instancePuncTimes[1]);
            }
            double puncThroughput = instancePatternPunctuation * vnfInstanceNum / ((puncEndTime - puncStartTime) / 1E9);
            puncThroughputMap.put(puncID, puncThroughput);
        }
    }


    private void computeTimeBreakdown() {
        double aggInstanceParseTime = 0;
        double aggInstanceSyncTime = 0;
        double aggInstanceUsefulTime = 0;
        double aggManagerSyncTime = 0;
        double aggManagerUsefulTime = 0;
        double aggCCSwitchTime = 0;
        double aggTotalTime = 0;

        /** Breakdown at instance level */
        for (int i = 0; i < vnfInstanceNum; i++) {
            VNFInstance sender = senderMap.get(i);
            aggInstanceParseTime = (double) sender.getAggParsingTime() / 1E6; // Millisecond
            aggInstanceSyncTime = (double) sender.getAGG_SYNC_TIME() / 1E6;
            aggInstanceUsefulTime = (double) sender.getAGG_USEFUL_TIME() / 1E6;
            aggCCSwitchTime += (double) sender.getAggCCSwitchTime() / 1E6;
            for (VNFRequest request : latencyMap.get(i)) {
                aggTotalTime += (double) (request.getFinishTime() - request.getCreateTime()) / 1E6;
            }
        }

        /** Breakdown at manager level */
        if (ccStrategy == 0) { // Partitioning
            aggManagerSyncTime = PartitionCCThread.getManagerEventSyncTime() / 1E6;
            aggManagerUsefulTime = PartitionCCThread.getManagerEventUsefulTime() / 1E6;
            // Caching time breakdown is done at instance level
        } else if (ccStrategy == 2) { // Offloading
            aggManagerUsefulTime = OffloadCCThread.getAggUsefulTime().get() / 1E6;
            aggInstanceSyncTime -= aggManagerUsefulTime;
        } else if (ccStrategy == 3) {
            //TODO: Get time breakdown from TPG threads
        } else if (ccStrategy == 4) { // OpenNF
            aggManagerUsefulTime = OpenNFController.getAggUsefulTime() / 1E6;
            aggInstanceSyncTime -= aggManagerUsefulTime;
        } else if (ccStrategy == 5) { // CHC
            aggManagerUsefulTime = CHCController.getAggUsefulTime() / 1E6;
            aggInstanceSyncTime -= aggManagerUsefulTime;
        } else if (ccStrategy == 7) {
            aggManagerSyncTime += PartitionCCThread.getManagerEventSyncTime() / 1E6;
            aggManagerSyncTime += OffloadCCThread.getAggSyncTime().get() / 1E6;
            //TODO: Get time breakdown from TPG threads
            aggManagerUsefulTime += PartitionCCThread.getManagerEventUsefulTime() / 1E6;
            aggManagerUsefulTime += OffloadCCThread.getAggUsefulTime().get() / 1E6;
            //TODO: Get time breakdown from TPG threads
        }

        totalParseTimeMS = aggInstanceParseTime;
        totalSyncTimeMS = aggInstanceSyncTime + aggManagerSyncTime;
        totalUsefulTimeMS = aggInstanceUsefulTime + aggManagerUsefulTime;
        totalCCSwitchTimeMS = aggCCSwitchTime;
        totalTimeMS = aggTotalTime;
    }

    private static void writeCSVDynamicThroughput(String pattern, String ccStrategy) {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
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
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
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
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
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

    private static void writeCSVScalability(String pattern, String ccStrategy, double throughput) {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "throughput");
        String directoryPath = String.format("%s/numInstance_%d/%s", baseDirectory, vnfInstanceNum, pattern);
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
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "breakdown");
        String directoryPath = String.format("%s/numInstance_%d/%s", baseDirectory, vnfInstanceNum, toPatternString(pattern));
        String filePath = String.format("%s/%s.csv", directoryPath, toStringStrategy(ccStrategy));
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
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
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

    public static String toStringStrategy(int ccStrategy) {
        if (ccStrategy == 0) {
            return "Partitioning";
        } else if (ccStrategy == 1) {
            return "Replication";
        } else if (ccStrategy == 2) {
            return "Offloading";
        } else if (ccStrategy == 3) {
            return "Preemptive";
        } else if (ccStrategy == 4) {
            return "OpenNF";
        } else if (ccStrategy == 5) {
            return "CHC";
        } else if (ccStrategy == 6) {
            return "S6";
        } else if (ccStrategy == 7) {
            return "TransNFV";
        } else {
            return "Invalid";
        }
    }

}
