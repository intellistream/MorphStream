package intellistream.morphstream.engine.txn.profiler;


import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.web.handler.WebSocketHandler;
import intellistream.morphstream.web.common.dao.BatchRuntimeData;
import intellistream.morphstream.web.common.dao.OverallTimeBreakdown;
import intellistream.morphstream.web.common.dao.TPGEdge;
import intellistream.morphstream.web.common.dao.TPGNode;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RuntimeMonitor extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitor.class);
    private static final RuntimeMonitor runtimeMonitor = new RuntimeMonitor();
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, DescriptiveStatistics[]>> opLatencyMap = new ConcurrentHashMap<>(); //operatorID -> latency[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> opThroughputMap = new ConcurrentHashMap<>(); //operatorID -> throughput[batchID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opBatchStartTimeMap = new ConcurrentHashMap<>(); //operatorID -> batchStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opBatchEndTimeMap = new ConcurrentHashMap<>(); //operatorID -> batchEndTime[batchID][threadID]
    // The following 8 are temporary execution time, updated upon each new event's processing
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPrepareStartTime = new ConcurrentHashMap<>(); //operatorID -> prepareStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPrepareTime = new ConcurrentHashMap<>(); //operatorID -> prepareTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPreExeStartTime = new ConcurrentHashMap<>(); //operatorID -> preprocessStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPreExeTime = new ConcurrentHashMap<>(); //operatorID -> preprocessTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opTxnStartTime = new ConcurrentHashMap<>(); //operatorID -> txnStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opTxnTime = new ConcurrentHashMap<>(); //operatorID -> txnTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPostStartTime = new ConcurrentHashMap<>(); //operatorID -> postStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPostTime = new ConcurrentHashMap<>(); //operatorID -> PostTime[batchID][threadID]
    // The following 2 keep record of summarized execution time for each event, updated after each event's processing finished
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, DescriptiveStatistics[]>> opStreamTimePerEvent = new ConcurrentHashMap<>(); //operatorID -> batchID -> stream processing time per batch
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, DescriptiveStatistics[]>> opTxnTimePerEvent = new ConcurrentHashMap<>(); //operatorID -> batchID -> txn time per batch
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentHashMap<TPGNode, List<TPGEdge>>>> opTPGMap = new ConcurrentHashMap<>(); //operatorID -> batchID -> TPG
    // General usages
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, AtomicInteger>> opNumThreadCompletedMap = new ConcurrentHashMap<>(); //operatorID -> num of threads that have submitted performance data
    private static final String[] operatorIDs = MorphStreamEnv.get().configuration().getString("operatorIDs").split(",");
    private static final String applicationID = MorphStreamEnv.get().configuration().getString("application");
    private static final HashMap<String, Long[]> opEmptyLongArrays = new HashMap<>(); //operatorID -> empty long array, used for quick creation of breakdown time arrays for each new batch
    private static final HashMap<String, Integer> operatorThreadNumMap = new HashMap<>(); //operatorID -> its thread number
    private static final BlockingQueue<Object> readyOperatorQueue = new LinkedBlockingQueue<>(); //ID of operators whose performance data is ready to be shown in the UI
    private static final EventLoopGroup bossGroup = new NioEventLoopGroup(); //for message transmission over websocket
    private static final EventLoopGroup workerGroup = new NioEventLoopGroup(2);
    private static final WebSocketHandler webSocketHandler = new WebSocketHandler();

    public static RuntimeMonitor get() {
        return runtimeMonitor;
    }

    public static void Initialize() {
        for (String operatorID : operatorIDs) {
            operatorThreadNumMap.put(operatorID, MorphStreamEnv.get().configuration().getInt("threadNumOf_" + operatorID, 4));
            opEmptyLongArrays.put(operatorID, new Long[operatorThreadNumMap.get(operatorID)]);

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

            opStreamTimePerEvent.put(operatorID, new ConcurrentHashMap<>());
            opTxnTimePerEvent.put(operatorID, new ConcurrentHashMap<>());
        }

        runtimeMonitor.start();
    }

    public void PREPARE_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> eventPrepareStartTimes = opPrepareStartTime.get(operatorID);
        eventPrepareStartTimes.putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        eventPrepareStartTimes.get(batchID)[threadID] = System.nanoTime();
    }

    public void ACC_PREPARE_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> eventPrepareTimes = opPrepareTime.get(operatorID);
        eventPrepareTimes.putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        eventPrepareTimes.get(batchID)[threadID] += System.nanoTime() - opPrepareStartTime.get(operatorID).get(batchID)[threadID];
    }

    public void PRE_EXE_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> eventPreExeStartTimes = opPreExeStartTime.get(operatorID);
        eventPreExeStartTimes.putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        eventPreExeStartTimes.get(batchID)[threadID] = System.nanoTime();
    }

    public void ACC_PRE_EXE_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> eventPreExeTimes = opPreExeTime.get(operatorID);
        eventPreExeTimes.putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        eventPreExeTimes.get(batchID)[threadID] += System.nanoTime() - opPreExeStartTime.get(operatorID).get(batchID)[threadID];
    }

    public void TXN_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> eventTxnStartTimes = opTxnStartTime.get(operatorID);
        eventTxnStartTimes.putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        eventTxnStartTimes.get(batchID)[threadID] = System.nanoTime();
    }

    public void TXN_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> eventTxnTimes = opTxnTime.get(operatorID);
        eventTxnTimes.putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        eventTxnTimes.get(batchID)[threadID] = System.nanoTime() - opTxnStartTime.get(operatorID).get(batchID)[threadID];
    }

    public void POST_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> eventPostStartTimes = opPostStartTime.get(operatorID);
        eventPostStartTimes.putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        eventPostStartTimes.get(batchID)[threadID] = System.nanoTime();
    }

    public void POST_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> eventPostTimes = opPostTime.get(operatorID);
        eventPostTimes.putIfAbsent(batchID, opEmptyLongArrays.get(operatorID));
        eventPostTimes.get(batchID)[threadID] = System.nanoTime() - opPostStartTime.get(operatorID).get(batchID)[threadID];
    }

    public void UPDATE_TPG(String operatorID, int batchID, TPGNode node, List<TPGEdge> edges) {
        ConcurrentHashMap<Integer, ConcurrentHashMap<TPGNode, List<TPGEdge>>> batchTPGMap = opTPGMap.get(operatorID);
        batchTPGMap.putIfAbsent(batchID, new ConcurrentHashMap<>());
        batchTPGMap.get(batchID).put(node, edges);
    }

    //TODO: Merge this with runtime submission method
    public void END_TOTAL_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event, keep record for all events' txn and stream-processing time
        ConcurrentHashMap<Integer, DescriptiveStatistics[]> eventStreamTimes = opStreamTimePerEvent.get(operatorID);
        ConcurrentHashMap<Integer, DescriptiveStatistics[]> eventTxnTimes = opTxnTimePerEvent.get(operatorID);
        eventStreamTimes.putIfAbsent(batchID, new DescriptiveStatistics[operatorThreadNumMap.get(operatorID)]);
        eventTxnTimes.putIfAbsent(batchID, new DescriptiveStatistics[operatorThreadNumMap.get(operatorID)]);
        if (eventStreamTimes.get(batchID)[threadID] == null) {
            eventStreamTimes.get(batchID)[threadID] = new DescriptiveStatistics();
        }
        if (eventTxnTimes.get(batchID)[threadID] == null) {
            eventTxnTimes.get(batchID)[threadID] = new DescriptiveStatistics();
        }
        long streamTime = opPrepareTime.get(operatorID).get(batchID)[threadID] + opPreExeTime.get(operatorID).get(batchID)[threadID] + opPostTime.get(operatorID).get(batchID)[threadID];
        long txnTime = opTxnTime.get(operatorID).get(batchID)[threadID];
        eventStreamTimes.get(batchID)[threadID].addValue(streamTime);
        eventTxnTimes.get(batchID)[threadID].addValue(txnTime);
    }

    public void submitRuntimeData(String operatorID, int batchID, int threadID, SynchronizedDescriptiveStatistics latencyStats, long batchStartTime, long batchEndTime) {
        LOG.info("Batch " + batchID + " runtime data received from operator " + operatorID + " thread " + threadID);
        int threadNum = operatorThreadNumMap.get(operatorID);

        ConcurrentHashMap<Integer, DescriptiveStatistics[]> batchLatencyStats = opLatencyMap.get(operatorID);
        ConcurrentHashMap<Integer, Long[]> batchStartTimeStats = opBatchStartTimeMap.get(operatorID);
        ConcurrentHashMap<Integer, Long[]> batchEndTimeStats = opBatchEndTimeMap.get(operatorID);
        ConcurrentHashMap<Integer, AtomicInteger> numThreadCompletedMap = opNumThreadCompletedMap.get(operatorID);

        batchLatencyStats.putIfAbsent(batchID, new DescriptiveStatistics[threadNum]);
        batchStartTimeStats.putIfAbsent(batchID, new Long[threadNum]);
        batchEndTimeStats.putIfAbsent(batchID, new Long[threadNum]);
        numThreadCompletedMap.putIfAbsent(batchID, new AtomicInteger(0));

        batchLatencyStats.get(batchID)[threadID] = latencyStats.copy();
        batchStartTimeStats.get(batchID)[threadID] = batchStartTime;
        batchEndTimeStats.get(batchID)[threadID] = batchEndTime;

        if (opNumThreadCompletedMap.get(operatorID).get(batchID).incrementAndGet() == threadNum) {
            LOG.info("Batch " + batchID + " runtime data received from all threads of operator " + operatorID);
            readyOperatorQueue.add(operatorID); // notify monitor to summarize this operator's runtime data and send to UI
        }
    }

    private void sendDataToFrontend(String operatorID) {
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
        int latestBatchID = opNumThreadCompletedMap.get(operatorID).keySet().stream().max(Integer::compare).orElseThrow(NullPointerException::new);

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
            totalStreamTime += (long) Objects.requireNonNull(opStreamTimePerEvent.get(operatorID).get(latestBatchID))[i].getSum();
            totalTxnTime += (long) Objects.requireNonNull(opTxnTimePerEvent.get(operatorID).get(latestBatchID))[i].getSum();
        }
        // throughput
        long batchStartTime = Arrays.stream(batchStartTimeArray).min().orElse(Long.MAX_VALUE);
        long batchEndTime = Arrays.stream(batchEndTimeArray).max().orElse(Long.MIN_VALUE);
        long actualBatchDuration = batchEndTime - batchStartTime;
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

        BatchRuntimeData batchRuntimeData = new BatchRuntimeData(applicationID, String.valueOf(operatorID),
                throughput, minLatency, maxLatency, avgLatency, totalBatchSize, actualBatchDuration,
                overallTimeBreakdown, opTPGMap.get(operatorID).get(latestBatchID));

        webSocketHandler.getBatchInfoSender().send(batchRuntimeData);

        //TODO: Store batchRuntimeData into file
    }

    @Override
    public void run() {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(webSocketHandler);
            Channel channel = bootstrap.bind(5001).sync().channel();

            while (true) {
                if (webSocketHandler.getBatchInfoSender().getContext() != null) { // Do not send data to frontend until the connection is established
                    try {
                        String operatorID = (String) readyOperatorQueue.take();
                        sendDataToFrontend(operatorID);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            channel.closeFuture().sync(); // block until server is closed
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}

