package intellistream.morphstream.api.input.simVNF;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;

public class VNFManager {
    private int stateStartID = 0;
    private int parallelism;
    private int stateRange;
    private int totalRequests;
    private String patternString;
    private static HashMap<Integer, VNFSenderThread> senderMap = new HashMap<>();
    private static HashMap<Integer, Thread> senderThreadMap = new HashMap<>();
    private static HashMap<Integer, ArrayList<Long>> latencyMap = new HashMap<>(); //instanceID -> instance's latency list
    private static HashMap<Integer, Double> throughputMap = new HashMap<>(); //instanceID -> instance's throughput
    private static HashMap<Integer, Double> minLatencyMap = new HashMap<>(); //instanceID -> instance's min latency
    private static HashMap<Integer, Double> maxLatencyMap = new HashMap<>(); //instanceID -> instance's max latency
    private static HashMap<Integer, Double> avgLatencyMap = new HashMap<>(); //instanceID -> instance's avg latency
    private static HashMap<Integer, Double> percentile95Map = new HashMap<>(); //instanceID -> instance's 95th percentile latency
    private int totalRequestCounter = 0;
    private long overallStartTime = Long.MAX_VALUE;
    private long overallEndTime = Long.MIN_VALUE;
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);

    public VNFManager(int totalRequests, int parallelism, int stateRange, int ccStrategy, int pattern) {
        this.totalRequests = totalRequests;
        this.parallelism = parallelism;
        this.stateRange = stateRange;
        this.patternString = patternTranslator(pattern);

        CyclicBarrier senderBarrier = new CyclicBarrier(parallelism);
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");

        for (int i = 0; i < parallelism; i++) {
            String csvFilePath = String.format(rootPath + "/pattern_files/%s/instance_%d.csv", patternString, i);
            int stateGap = stateRange / parallelism;
            VNFSenderThread sender = new VNFSenderThread(i, ccStrategy,
                    stateStartID + i * stateGap, stateStartID + (i + 1) * stateGap, stateRange,
                    csvFilePath, senderBarrier, totalRequests /parallelism);
            Thread senderThread = new Thread(sender);
            senderMap.put(i, sender);
            senderThreadMap.put(i, senderThread);
        }
    }

    public static VNFSenderThread getSender(int id) {
        return senderMap.get(id);
    }
    public static HashMap<Integer, VNFSenderThread> getSenderMap() {
        return senderMap;
    }

    public void startVNFInstances() {
        for (int i = 0; i < parallelism; i++) {
            senderThreadMap.get(i).start();
        }
    }

    public double joinVNFInstances() {
        for (int i = 0; i < parallelism; i++) {
            try {
                senderThreadMap.get(i).join();
                totalRequestCounter += senderMap.get(i).getFinishedRequestCount();
                overallStartTime = Math.min(overallStartTime, senderMap.get(i).getOverallStartTime());
                overallEndTime = Math.max(overallEndTime, senderMap.get(i).getOverallEndTime());
                latencyMap.put(i, senderMap.get(i).getLatencyList());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        SynchronizedDescriptiveStatistics instanceLatencyStats = new SynchronizedDescriptiveStatistics();
        for (int i = 0; i < parallelism; i++) {
            instanceLatencyStats.clear();
            for (long latency : latencyMap.get(i)) {
                instanceLatencyStats.addValue(latency); //TODO: Sorting request order based on requestID???
            }
            double minLatency = instanceLatencyStats.getMin();
            double maxLatency = instanceLatencyStats.getMax();
            double avgLatency = instanceLatencyStats.getMean();
            double percentile95 = instanceLatencyStats.getPercentile(95);
        }

        //TODO: Write latency data and stats to csv file

        long overallDuration = overallEndTime - overallStartTime;
        double overallThroughput = totalRequestCounter / (overallDuration / 1E9);
        return overallThroughput;
    }

    private String patternTranslator(int pattern) {
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

}
