package intellistream.morphstream.engine.txn.profiler;


import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import java.util.concurrent.*;

public class RuntimeMonitor extends Thread {
    private static final RuntimeMonitor runtimeMonitor = new RuntimeMonitor();
    private static final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, DescriptiveStatistics[]>> batchLatencyMap = new ConcurrentHashMap<>(); //batchID -> (operatorID -> DescriptiveStatistics[threadID])
    private static final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double[]>> batchThroughputMap = new ConcurrentHashMap<>(); //batchID -> (operatorID -> throughput[threadID])
    private final String[] operatorIDs = MorphStreamEnv.get().configuration().getString("operatorIDs").split(",");
    private int batchToShow = 0; //ID of batch whose runtime data will be shown in the UI

    public void submitRuntimeData(int batchID, int operatorID, int threadID, DescriptiveStatistics latencyStats, double throughput) {
        assert (batchID != batchToShow);
        batchLatencyMap.putIfAbsent(batchID, new ConcurrentHashMap<>());
        batchLatencyMap.get(batchID).putIfAbsent(operatorID, new DescriptiveStatistics[operatorIDs.length]);
        batchLatencyMap.get(batchID).get(operatorID)[threadID] = latencyStats;

        batchThroughputMap.putIfAbsent(batchID, new ConcurrentHashMap<>());
        batchThroughputMap.get(batchID).putIfAbsent(operatorID, new Double[operatorIDs.length]);
        batchThroughputMap.get(batchID).get(operatorID)[threadID] = throughput;

        if (batchLatencyMap.get(batchID).size() == operatorIDs.length) {
            this.notify();
        }
    }

    private void computeRuntimeStats() {
        //TODO: Compute summary of runtime data
    }

    @Override
    public void run() {
        while (true) {
            try {
                this.wait(); // Wait for notification
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            batchToShow += 1;
            //TODO: Submit runtime information to frontend
        }
    }

    public static RuntimeMonitor get() {
        return runtimeMonitor;
    }

}

