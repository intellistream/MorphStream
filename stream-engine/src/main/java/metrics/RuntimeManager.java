package metrics;

import components.Topology;
import components.operators.api.TransactionalBolt;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static content.common.CommonMetaTypes.kMaxThreadNum;

class RuntimeManager {
    private static ThreadPoolExecutor threadPool;
    private static final HashMap<String, RuntimeMeasureThread[]> measureThreadsMap = new HashMap<>();
    private static AtomicBoolean signaled;
    private static final HashMap<String, DescriptiveStatistics[]> latencyMap = new HashMap<>();
    private static final HashMap<String, List<Double>> throughputMap = new HashMap<>();

    public void Initialize(Topology topology) {
        threadPool = new ThreadPoolExecutor(kMaxThreadNum, kMaxThreadNum, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        for (String operatorID : topology.getComponentIds()) {
            int numThreads = topology.getComponent(operatorID).getNumTasks();
            measureThreadsMap.put(operatorID, new RuntimeMeasureThread[numThreads]);
            latencyMap.put(operatorID, new DescriptiveStatistics[numThreads]);
            throughputMap.put(operatorID, new ArrayList<>());

            for (int threadID = 0; threadID < numThreads; threadID++) {
                latencyMap.get(operatorID)[threadID] = new DescriptiveStatistics();
                TransactionalBolt bolt = (TransactionalBolt) topology.getComponent(operatorID).getOp(); //TODO: Get bolt thread from topology
                measureThreadsMap.get(operatorID)[threadID] = new RuntimeMeasureThread(operatorID, threadID, bolt);
                threadPool.execute(measureThreadsMap.get(operatorID)[threadID]);
            }
        }

        // Initialize AtomicBoolean to track whether a signal has been sent in this cycle
        signaled = new AtomicBoolean(false);

        // Schedule the signal-sending task to run every second
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::sendSignals, 0, 1, TimeUnit.SECONDS);
    }

    // Send signals to measure threads
    public void sendSignals() {
        if (signaled.compareAndSet(false, true)) {
            for (RuntimeMeasureThread[] measureThreads : measureThreadsMap.values()) {
                for (RuntimeMeasureThread measureThread : measureThreads) {
                    measureThread.signal();
                }
            }
        }
    }

    // Update performance info by measure threads
    public static void updateLatencyStats(String operatorID, int threadID, DescriptiveStatistics stats) {
        latencyMap.get(operatorID)[threadID] = stats;
    }

    public static void updateThroughputStats(String operatorID, int threadID, double stats) {
        throughputMap.get(operatorID).add(threadID, stats);
    }
}

