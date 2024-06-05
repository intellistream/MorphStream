package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
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
    private static HashMap<Integer, Double> throughputMap = new HashMap<>(); //instanceID -> instance's throughput
    private static HashMap<Integer, Double> minLatencyMap = new HashMap<>(); //instanceID -> instance's min latency
    private static HashMap<Integer, Double> maxLatencyMap = new HashMap<>(); //instanceID -> instance's max latency
    private static HashMap<Integer, Double> avgLatencyMap = new HashMap<>(); //instanceID -> instance's avg latency
    private static HashMap<Integer, Double> percentile95Map = new HashMap<>(); //instanceID -> instance's 95th percentile latency
    private int totalRequestCounter = 0;
    private long overallStartTime = Long.MAX_VALUE;
    private long overallEndTime = Long.MIN_VALUE;
    private final int totalRequests = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private static final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final int stateRange = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private static final int ccStrategy = MorphStreamEnv.get().configuration().getInt("ccStrategy");
    private static final int pattern = MorphStreamEnv.get().configuration().getInt("workloadPattern");
    private static final int numTPGThreads = MorphStreamEnv.get().configuration().getInt("tthread");
    private static final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private static long totalParseTimeNS = 0;
    private static long totalSyncTimeNS = 0;
    private static long totalUsefulTimeNS = 0;
    private static long totalCCSwitchTimeNS = 0;

    public VNFRunner() {
        this.patternString = toPatternString(pattern);
        CyclicBarrier senderBarrier = new CyclicBarrier(vnfInstanceNum);
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");

        for (int i = 0; i < vnfInstanceNum; i++) {
            String csvFilePath = String.format(rootPath + "/pattern_files/%s/instanceNum_%d/%s/instance_%d.csv", experimentID, vnfInstanceNum, patternString, i);
            int stateGap = stateRange / vnfInstanceNum;
            int instanceExpRequestCount = totalRequests / vnfInstanceNum;
            VNFInstance sender = new VNFInstance(i,
                    stateStartID + i * stateGap, stateStartID + (i + 1) * stateGap, stateRange,
                    ccStrategy, numTPGThreads, csvFilePath, senderBarrier, instanceExpRequestCount);
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
            case "5.1":
                writeCSVThroughput(patternString, ccStrategyString, overallThroughput);
                break;
            case "5.2.1":
                writeCSVThroughput(patternString, ccStrategyString, overallThroughput);
                writeCSVLatency(patternString, ccStrategyString);
                break;
            case "5.2.2":
                writeCSVScalability(patternString, ccStrategyString, overallThroughput);
                break;
            case "5.3.1":
                computeTimeBreakdown();
                writeCSVBreakdown();
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
            String lineToWrite = totalParseTimeNS + "," + totalSyncTimeNS + "," + totalUsefulTimeNS + "," + totalCCSwitchTimeNS + "\n";
            fileWriter.write(lineToWrite);
            System.out.println("Data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
        }
    }

    private void computeTimeBreakdown() {
        long aggInstanceParseTime = 0;
        long aggInstanceSyncTime = 0;
        long aggInstanceUsefulTime = 0;
        long aggManagerSyncTime = 0;
        long aggManagerUsefulTime = 0;
        long aggCCSwitchTime = 0;

        for (int i = 0; i < vnfInstanceNum; i++) {
            VNFInstance sender = senderMap.get(i);
            for (long parseTime : sender.getAggParsingTimeMap().values()) {
                aggInstanceParseTime += parseTime;
            }
            for (long syncTime : sender.getAggInstanceSyncTimeMap().values()) {
                aggInstanceSyncTime += syncTime;
            }
            for (long usefulTime : sender.getAggInstanceUsefulTimeMap().values()) {
                aggInstanceUsefulTime += usefulTime;
            }
            aggCCSwitchTime += sender.getAggCCSwitchTime();
        }

        if (ccStrategy == 0) { // Partitioning
            aggManagerSyncTime = PartitionCCThread.getManagerEventSyncTime();
            aggManagerUsefulTime = PartitionCCThread.getManagerEventUsefulTime();
            // Caching time breakdown is done at instance level
        } else if (ccStrategy == 2) { // Offloading
            aggManagerSyncTime = OffloadCCThread.getAggSyncTime().get();
            aggManagerUsefulTime = OffloadCCThread.getAggUsefulTime().get();
        } else if (ccStrategy == 3) {
            //TODO: Get time breakdown from TPG threads
        } else if (ccStrategy == 4) { // OpenNF
            aggManagerSyncTime = OpenNFController.getAggSyncTime();
            aggManagerUsefulTime = OpenNFController.getAggUsefulTime();
        } else if (ccStrategy == 5) {
            aggManagerSyncTime = CHCController.getAggSyncTime();
            aggManagerUsefulTime = CHCController.getAggUsefulTime();
        } else if (ccStrategy == 6) {
            aggManagerSyncTime += PartitionCCThread.getManagerEventSyncTime();
            aggManagerSyncTime += OffloadCCThread.getAggSyncTime().get();
            //TODO: Get time breakdown from TPG threads

            aggManagerUsefulTime += PartitionCCThread.getManagerEventUsefulTime();
            aggManagerUsefulTime += OffloadCCThread.getAggUsefulTime().get();
            //TODO: Get time breakdown from TPG threads
        }

        totalParseTimeNS = aggInstanceParseTime;
        totalSyncTimeNS = aggInstanceSyncTime + aggManagerSyncTime;
        totalUsefulTimeNS = aggInstanceUsefulTime + aggManagerUsefulTime;
        totalCCSwitchTimeNS = aggCCSwitchTime;
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

    private String toPatternString(int pattern) {
        switch (pattern) {
            case 0:
                return "loneOperative";
            case 1:
                return "sharedReaders";
            case 2:
                return "sharedWriters";
            case 3:
                return "mutualInteractive";
            default:
                return "invalid";
        }
    }

    public String toStringStrategy(int ccStrategy) {
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
            return "Adaptive";
        } else {
            return "Invalid";
        }
    }

}
