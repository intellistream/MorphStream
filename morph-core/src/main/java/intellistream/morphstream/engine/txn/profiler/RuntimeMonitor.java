package intellistream.morphstream.engine.txn.profiler;


import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.ObjectMapper;
import communication.dao.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RuntimeMonitor extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitor.class);
    private static final RuntimeMonitor runtimeMonitor = new RuntimeMonitor();
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, DescriptiveStatistics[]>> opLatencyMap = new ConcurrentHashMap<>(); //operatorID -> latency[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> opThroughputMap = new ConcurrentHashMap<>(); //operatorID -> throughput[batchID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opBatchStartTimeMap = new ConcurrentHashMap<>(); //operatorID -> batchStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opBatchEndTimeMap = new ConcurrentHashMap<>(); //operatorID -> batchEndTime[batchID][threadID]
    // The following 8 are temporary execution time, updated upon each new event's processing
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opPrepareStartTime = new ConcurrentHashMap<>(); //operatorID -> prepareStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opPrepareTime = new ConcurrentHashMap<>(); //operatorID -> prepareTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opPreExeStartTime = new ConcurrentHashMap<>(); //operatorID -> preprocessStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opPreExeTime = new ConcurrentHashMap<>(); //operatorID -> preprocessTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opTxnStartTime = new ConcurrentHashMap<>(); //operatorID -> txnStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opTxnTime = new ConcurrentHashMap<>(); //operatorID -> txnTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opPostStartTime = new ConcurrentHashMap<>(); //operatorID -> postStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opPostTime = new ConcurrentHashMap<>(); //operatorID -> PostTime[batchID][threadID]
    // The following 2 keep record of summarized execution time for each event, updated after each event's processing finished
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opStreamTimePerBatch = new ConcurrentHashMap<>(); //operatorID -> batchID -> stream processing time per batch
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, long[]>> opTxnTimePerBatch = new ConcurrentHashMap<>(); //operatorID -> batchID -> txn time per batch
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> opAccTimeMap = new ConcurrentHashMap<>(); //operatorID -> batchID -> overall latency until each batch
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> opAccNumEventsMap = new ConcurrentHashMap<>(); //operatorID -> batchID -> overall throughput until each batch
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentHashMap<TPGNode, List<TPGEdge>>>> opTPGMap = new ConcurrentHashMap<>(); //operatorID -> batchID -> TPG
    // General usages
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, AtomicInteger>> opNumThreadCompletedMap = new ConcurrentHashMap<>(); //operatorID -> num of threads that have submitted performance data
    private static final String[] operatorIDs = MorphStreamEnv.get().configuration().getString("operatorIDs").split(",");
    private static final String applicationID = MorphStreamEnv.get().configuration().getString("application");
    private static final HashMap<String, long[]> opEmptyLongArrays = new HashMap<>(); //operatorID -> empty long array, used for quick creation of breakdown time arrays for each new batch
    private static final HashMap<String, Integer> operatorThreadNumMap = new HashMap<>(); //operatorID -> its thread number
    private static final HashMap<String, Integer> operatorBatchNumMap = new HashMap<>(); //operatorID -> current batch whose runtime is going to be written to file
    private static final BlockingQueue<Object> readyOperatorQueue = new LinkedBlockingQueue<>(); //ID of operators whose performance data is ready to be shown in the UI
    private static final EventLoopGroup bossGroup = new NioEventLoopGroup(); //for message transmission over websocket
    private static final EventLoopGroup workerGroup = new NioEventLoopGroup(2);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Batch>> batchedData = new ConcurrentHashMap<>();
    private final String dataPath = "data/jobs";

    public static RuntimeMonitor get() {
        return runtimeMonitor;
    }

    public static void Initialize() {
        for (String operatorID : operatorIDs) {
            operatorThreadNumMap.put(operatorID, MorphStreamEnv.get().configuration().getInt("threadNumOf_" + operatorID, 4));
            operatorBatchNumMap.put(operatorID, 1);
            opEmptyLongArrays.put(operatorID, new long[operatorThreadNumMap.get(operatorID)]);

            opLatencyMap.put(operatorID, new ConcurrentHashMap<>());
            opThroughputMap.put(operatorID, new ConcurrentHashMap<>());
            opBatchStartTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opBatchEndTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opNumThreadCompletedMap.put(operatorID, new ConcurrentHashMap<>());

            opPrepareStartTime.put(operatorID, new ConcurrentHashMap<>());
            opPrepareTime.put(operatorID, new ConcurrentHashMap<>());
            opPreExeStartTime.put(operatorID, new ConcurrentHashMap<>());
            opPreExeTime.put(operatorID, new ConcurrentHashMap<>());
            opTxnStartTime.put(operatorID, new ConcurrentHashMap<>());
            opTxnTime.put(operatorID, new ConcurrentHashMap<>());
            opPostStartTime.put(operatorID, new ConcurrentHashMap<>());
            opPostTime.put(operatorID, new ConcurrentHashMap<>());
            opTPGMap.put(operatorID, new ConcurrentHashMap<>());

            opStreamTimePerBatch.put(operatorID, new ConcurrentHashMap<>());
            opTxnTimePerBatch.put(operatorID, new ConcurrentHashMap<>());
            opAccTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opAccNumEventsMap.put(operatorID, new ConcurrentHashMap<>());
        }

        runtimeMonitor.start();
    }

    public void PREPARE_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        opPrepareStartTime.get(operatorID).putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        opPrepareStartTime.get(operatorID).get(batchID)[threadID] = System.nanoTime();
    }

    public void ACC_PREPARE_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        opPrepareTime.get(operatorID).putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        opPrepareTime.get(operatorID).get(batchID)[threadID] += System.nanoTime() - opPrepareStartTime.get(operatorID).get(batchID)[threadID];
    }

    public void PRE_EXE_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        opPreExeStartTime.get(operatorID).putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        opPreExeStartTime.get(operatorID).get(batchID)[threadID] = System.nanoTime();
    }

    public void ACC_PRE_EXE_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        opPreExeTime.get(operatorID).putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        opPreExeTime.get(operatorID).get(batchID)[threadID] += System.nanoTime() - opPreExeStartTime.get(operatorID).get(batchID)[threadID];
    }

    public void TXN_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per batch
        opTxnStartTime.get(operatorID).putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        opTxnStartTime.get(operatorID).get(batchID)[threadID] = System.nanoTime();
    }

    public void TXN_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per batch
        opTxnTime.get(operatorID).putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        opTxnTime.get(operatorID).get(batchID)[threadID] = System.nanoTime() - opTxnStartTime.get(operatorID).get(batchID)[threadID];
    }

    public void POST_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per batch
        opPostStartTime.get(operatorID).putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        opPostStartTime.get(operatorID).get(batchID)[threadID] = System.nanoTime();
    }

    public void POST_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per batch
        opPostTime.get(operatorID).putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        opPostTime.get(operatorID).get(batchID)[threadID] = System.nanoTime() - opPostStartTime.get(operatorID).get(batchID)[threadID];
    }

    public void UPDATE_TPG(String operatorID, int batchID, TPGNode node, List<TPGEdge> edges) {
        opTPGMap.get(operatorID).putIfAbsent(batchID, new ConcurrentHashMap<>());
        opTPGMap.get(operatorID).get(batchID).put(node, edges);
    }

    //TODO: Merge this with runtime submission method
    public void END_TOTAL_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per batch, keep record for all events' txn and stream-processing time
        opStreamTimePerBatch.get(operatorID).putIfAbsent(batchID, new long[operatorThreadNumMap.get(operatorID)]);
        opTxnTimePerBatch.get(operatorID).putIfAbsent(batchID, new long[operatorThreadNumMap.get(operatorID)]);
        long streamTime = opPrepareTime.get(operatorID).get(batchID)[threadID] + opPreExeTime.get(operatorID).get(batchID)[threadID] + opPostTime.get(operatorID).get(batchID)[threadID];
        long txnTime = opTxnTime.get(operatorID).get(batchID)[threadID];
        if (streamTime == 0 || txnTime == 0) {
            LOG.info("Stream or txn time is 0 for operator " + operatorID + " batch " + batchID + " thread " + threadID);
        }
        opStreamTimePerBatch.get(operatorID).get(batchID)[threadID] = streamTime;
        opTxnTimePerBatch.get(operatorID).get(batchID)[threadID] = txnTime;
    }

    public void submitRuntimeData(String operatorID, int batchID, int threadID, SynchronizedDescriptiveStatistics latencyStats, long batchStartTime, long batchEndTime) {
//        LOG.info("Batch " + batchID + " runtime data received from operator " + operatorID + " thread " + threadID);
        int threadNum = operatorThreadNumMap.get(operatorID);

        opLatencyMap.get(operatorID).putIfAbsent(batchID, new DescriptiveStatistics[threadNum]);
        opBatchStartTimeMap.get(operatorID).putIfAbsent(batchID, new long[threadNum]);
        opBatchEndTimeMap.get(operatorID).putIfAbsent(batchID, new long[threadNum]);
        opNumThreadCompletedMap.get(operatorID).putIfAbsent(batchID, new AtomicInteger(0));

        opLatencyMap.get(operatorID).get(batchID)[threadID] = latencyStats.copy();
        opBatchStartTimeMap.get(operatorID).get(batchID)[threadID] = batchStartTime;
        opBatchEndTimeMap.get(operatorID).get(batchID)[threadID] = batchEndTime;

        if (opNumThreadCompletedMap.get(operatorID).get(batchID).incrementAndGet() == threadNum) {
            LOG.info("Batch " + batchID + " runtime data received from all threads of operator " + operatorID);
            readyOperatorQueue.add(operatorID); // notify monitor to summarize this operator's runtime data and send to UI
        }
    }

    private void saveBatchData(String operatorID) {
        // summarize runtime data before sending
        int threadNum = operatorThreadNumMap.get(operatorID);
        long[] batchStartTimeArray = new long[threadNum];
        long[] batchEndTimeArray = new long[threadNum];
        long[] batchDurationArray = new long[threadNum];
        double[] avgLatencyArray = new double[threadNum];
        double[] minLatencyArray = new double[threadNum];
        double[] maxLatencyArray = new double[threadNum];
        long[] batchSizeArray = new long[threadNum];
        long totalBatchSize = 0; // total number of events processed by this operator in the latest batch
        long totalStreamTime = 0;
        long totalTxnTime = 0;
//        int latestBatchID = opNumThreadCompletedMap.get(operatorID).keySet().stream().max(Integer::compare).orElseThrow(NullPointerException::new);
        int latestBatchID = operatorBatchNumMap.get(operatorID);
        operatorBatchNumMap.put(operatorID, latestBatchID + 1);

        for (int i=0; i<threadNum; i++) {
            // throughput and latency
            batchStartTimeArray[i] = Objects.requireNonNull(opBatchStartTimeMap.get(operatorID).get(latestBatchID))[i];
            batchEndTimeArray[i] = Objects.requireNonNull(opBatchEndTimeMap.get(operatorID).get(latestBatchID))[i];
            batchDurationArray[i] = batchEndTimeArray[i] - batchStartTimeArray[i];
            DescriptiveStatistics[] batchLatencyStats = Objects.requireNonNull(opLatencyMap.get(operatorID).get(latestBatchID));
            avgLatencyArray[i] = batchLatencyStats[i].getMean();
            minLatencyArray[i] = batchLatencyStats[i].getMin();
            maxLatencyArray[i] = batchLatencyStats[i].getMax();
            batchSizeArray[i] = batchLatencyStats[i].getN(); //batchSize of each thread
            totalBatchSize += batchSizeArray[i];
            // overall execution time breakdown
            try {
                totalStreamTime += opStreamTimePerBatch.get(operatorID).get(latestBatchID)[i];
            } catch (NullPointerException e) {
                Log.info("Stream time is null for operator " + operatorID + " batch " + latestBatchID + " thread " + i);
                throw e;
            }
            try {
                totalTxnTime += opTxnTimePerBatch.get(operatorID).get(latestBatchID)[i];
            } catch (NullPointerException e) {
                Log.info("Txn time is null for operator " + operatorID + " batch " + latestBatchID + " thread " + i);
                throw e;
            }
        }
        // throughput
        long batchStartTime = Arrays.stream(batchStartTimeArray).min().orElse(Long.MAX_VALUE);
        long batchEndTime = Arrays.stream(batchEndTimeArray).max().orElse(Long.MIN_VALUE);
        long actualBatchDuration = batchEndTime - batchStartTime;
        if (latestBatchID > 1) {
            opAccTimeMap.get(operatorID).put(latestBatchID, opAccTimeMap.get(operatorID).get(latestBatchID - 1) + actualBatchDuration);
            opAccNumEventsMap.get(operatorID).put(latestBatchID, opAccNumEventsMap.get(operatorID).get(latestBatchID - 1) + totalBatchSize);
        } else {
            opAccTimeMap.get(operatorID).put(latestBatchID, actualBatchDuration);
            opAccNumEventsMap.get(operatorID).put(latestBatchID, totalBatchSize);
        }
        double accumulativeLatency = opAccTimeMap.get(operatorID).get(latestBatchID) * 1E-9 / opAccNumEventsMap.get(operatorID).get(latestBatchID);
        double accumulativeThroughput = 1 / accumulativeLatency;

        long batchDurationSum = Arrays.stream(batchDurationArray).sum();
        double throughput = totalBatchSize * 1E9 / batchDurationSum;
        opThroughputMap.get(operatorID).put(latestBatchID, throughput); // keep record for throughput history
        // latency
        double latencySum = 0;
        for (int i=0; i<threadNum; i++) {
            latencySum += avgLatencyArray[i] * batchSizeArray[i];
        }
        double avgLatency = latencySum / totalBatchSize;
        double minLatency = Arrays.stream(minLatencyArray).min().orElse(Double.MAX_VALUE);
        double maxLatency = Arrays.stream(maxLatencyArray).max().orElse(Double.MIN_VALUE);
        // overall execution breakdown
        long avgStreamTime = totalStreamTime / totalBatchSize;
        long avgTxnTime = totalTxnTime / totalBatchSize;
        long avgTotalTime = batchDurationSum / totalBatchSize;
        long avgOverheadTime = avgTotalTime - avgStreamTime - avgTxnTime;
        OverallTimeBreakdown overallTimeBreakdown = new OverallTimeBreakdown(avgTotalTime, avgStreamTime, avgTxnTime, avgOverheadTime);

        double explore_time = 0;
        double useful_time = 0;
        double abort_time = 0;
        double construct_time = 0;
        double tracking_time = 0;
        for (int threadId = 0; threadId < threadNum; threadId++) {
            explore_time += Metrics.Scheduler_Record.Explore[threadId].getMean();
//            useful_time += Metrics.Scheduler_Record.Useful[threadId].getMean();
            abort_time += Metrics.Scheduler_Record.Abort[threadId].getMean();
            construct_time += Metrics.Scheduler_Record.Construct[threadId].getMean();
            tracking_time += Metrics.Scheduler_Record.Tracking[threadId].getMean();
        }
        useful_time = Metrics.Scheduler_Record.Useful[0].getMean();
        explore_time = explore_time / threadNum;
//        useful_time = useful_time / threadNum;
        abort_time = abort_time / threadNum;
        construct_time = construct_time / threadNum;
        tracking_time = tracking_time / threadNum;
        SchedulerTimeBreakdown schedulerTimeBreakdown = new SchedulerTimeBreakdown(explore_time, useful_time, abort_time, construct_time, tracking_time);

        Batch batch = new Batch("3", String.valueOf(operatorID),
                throughput, minLatency, maxLatency, avgLatency,
                totalBatchSize, actualBatchDuration,
                accumulativeLatency, accumulativeThroughput,
                overallTimeBreakdown, schedulerTimeBreakdown, opTPGMap.get(operatorID).get(latestBatchID), latestBatchID);

        try {
            File directory = new File(String.format("%s/%s/%s", dataPath, "3", operatorID));
            if (!directory.exists()) {
                if (directory.mkdirs()) {
                    Log.info("Directory created successfully.");
                } else {
                    Log.info("Failed to create directory.");
                    return;
                }
            }
            batchedData.putIfAbsent(operatorID, new ConcurrentHashMap<>());
            batchedData.get(operatorID).put(latestBatchID, batch);
//            objectMapper.writeValue(new File(String.format("%s/%s/%s/%d.json", dataPath, applicationID, operatorID, latestBatchID)), batchRuntimeData);
            objectMapper.writeValue(new File(String.format("%s/%s/%s/%d.json", dataPath, "3", operatorID, latestBatchID)), batch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Batch getBatchedDataByBatch(int batch, String operatorId) {
        ConcurrentHashMap<Integer, Batch> operatorBatch = RuntimeMonitor.batchedData.get(operatorId);
        if (operatorBatch == null) {
            return null;
        } else {
            return operatorBatch.get(batch);
        }
    }

    int batchCount = 0;

    @Override
    public void run() {
        try {
            while (true) {
                String operatorID = (String) readyOperatorQueue.take();
                saveBatchData(operatorID);
                batchCount = batchCount + 1;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
