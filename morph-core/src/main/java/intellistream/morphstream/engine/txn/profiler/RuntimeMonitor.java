package intellistream.morphstream.engine.txn.profiler;


import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.web.WebSocketHandler;
import intellistream.morphstream.web.common.dao.BatchRuntimeData;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
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
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opStartTimeMap = new ConcurrentHashMap<>(); //operatorID -> startTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPrepareStartTimeMap = new ConcurrentHashMap<>(); //operatorID -> prepareStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPrepareTimeMap = new ConcurrentHashMap<>(); //operatorID -> prepareTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPreExeStartTimeMap = new ConcurrentHashMap<>(); //operatorID -> preprocessStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPreExeTimeMap = new ConcurrentHashMap<>(); //operatorID -> preprocessTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opTxnStartTimeMap = new ConcurrentHashMap<>(); //operatorID -> txnStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opTxnTimeMap = new ConcurrentHashMap<>(); //operatorID -> txnTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPostStartTimeMap = new ConcurrentHashMap<>(); //operatorID -> postStartTime[batchID][threadID]
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long[]>> opPostTimeMap = new ConcurrentHashMap<>(); //operatorID -> PostTime[batchID][threadID]

    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, AtomicInteger>> opNumThreadCompletedMap = new ConcurrentHashMap<>(); //operatorID -> num of threads that have submitted performance data

    private static final String[] operatorIDs = MorphStreamEnv.get().configuration().getString("operatorIDs").split(",");
    private static final String applicationID = MorphStreamEnv.get().configuration().getString("application");
    private static final HashMap<String, Integer> operatorThreadNumMap = new HashMap<>(); //operatorID -> its thread number
    private static final BlockingQueue<Object> readyOperatorQueue = new LinkedBlockingQueue<>(); //ID of operators whose performance data is ready to be shown in the UI
    EventLoopGroup bossGroup = new NioEventLoopGroup(); //for message transmission over websocket
    EventLoopGroup workerGroup = new NioEventLoopGroup(2);
    WebSocketHandler webSocketHandler = new WebSocketHandler();

    public static RuntimeMonitor get() {
        return runtimeMonitor;
    }

    public static void Initialize() {
        for (String operatorID : operatorIDs) {
            operatorThreadNumMap.put(operatorID, MorphStreamEnv.get().configuration().getInt("threadNumOf_" + operatorID, 4));
            opLatencyMap.put(operatorID, new ConcurrentHashMap<>());
            opThroughputMap.put(operatorID, new ConcurrentHashMap<>());
            opBatchStartTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opBatchEndTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opNumThreadCompletedMap.put(operatorID, new ConcurrentHashMap<>());

            opStartTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opPrepareStartTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opPrepareTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opPreExeStartTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opPreExeTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opTxnStartTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opTxnTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opPostStartTimeMap.put(operatorID, new ConcurrentHashMap<>());
            opPostTimeMap.put(operatorID, new ConcurrentHashMap<>());
        }

        runtimeMonitor.start();
    }

    public void TOTAL_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // record start time of each batch
        ConcurrentHashMap<Integer, Long[]> batchStartTimeMap = opStartTimeMap.get(operatorID);
        batchStartTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        if (batchStartTimeMap.get(batchID)[threadID] == 0) {
            batchStartTimeMap.get(batchID)[threadID] = System.nanoTime(); //TODO: Reset to zero at the end of each batch
        }
    }

    public void PREPARE_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> batchPrepareStartTimeMap = opPrepareStartTimeMap.get(operatorID);
        batchPrepareStartTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        batchPrepareStartTimeMap.get(batchID)[threadID] = System.nanoTime();
    }

    public void ACC_PREPARE_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> batchPrepareTimeMap = opPrepareTimeMap.get(operatorID);
        batchPrepareTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        batchPrepareTimeMap.get(batchID)[threadID] += System.nanoTime() - opPrepareStartTimeMap.get(operatorID).get(batchID)[threadID];
    }

    public void PRE_EXE_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> batchPreExeStartTimeMap = opPreExeStartTimeMap.get(operatorID);
        batchPreExeStartTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        batchPreExeStartTimeMap.get(batchID)[threadID] = System.nanoTime();
    }

    public void ACC_PRE_EXE_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> batchPreExeTimeMap = opPreExeTimeMap.get(operatorID);
        batchPreExeTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        batchPreExeTimeMap.get(batchID)[threadID] += System.nanoTime() - opPreExeStartTimeMap.get(operatorID).get(batchID)[threadID];
    }

    public void TXN_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> batchTxnStartTimeMap = opTxnStartTimeMap.get(operatorID);
        batchTxnStartTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        batchTxnStartTimeMap.get(batchID)[threadID] = System.nanoTime();
    }

    public void TXN_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> batchTxnTimeMap = opTxnTimeMap.get(operatorID);
        batchTxnTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        batchTxnTimeMap.get(batchID)[threadID] = System.nanoTime() - opTxnStartTimeMap.get(operatorID).get(batchID)[threadID];
    }

    public void POST_START_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> batchPostStartTimeMap = opPostStartTimeMap.get(operatorID);
        batchPostStartTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        batchPostStartTimeMap.get(batchID)[threadID] = System.nanoTime();
    }

    public void POST_TIME_MEASURE(String operatorID, int batchID, int threadID) { // per event
        ConcurrentHashMap<Integer, Long[]> batchPostTimeMap = opPostTimeMap.get(operatorID);
        batchPostTimeMap.putIfAbsent(batchID, new Long[operatorThreadNumMap.get(operatorID)]);
        batchPostTimeMap.get(batchID)[threadID] = System.nanoTime() - opPostStartTimeMap.get(operatorID).get(batchID)[threadID];
    }

    public void submitRuntimeData(String operatorID, int batchID, int threadID, DescriptiveStatistics latencyStats, long batchStartTime, long batchEndTime) {
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

        batchLatencyStats.get(batchID)[threadID] = latencyStats;
        batchStartTimeStats.get(batchID)[threadID] = batchStartTime;
        batchEndTimeStats.get(batchID)[threadID] = batchEndTime;

        if (numThreadCompletedMap.get(batchID).incrementAndGet() == threadNum) {
            readyOperatorQueue.add(operatorID); // notify monitor to summarize this operator's runtime data and send to UI
        }
    }

    private void sendDataToFrontend(int operatorID) {
        // summarize runtime data before sending
        int threadNum = operatorThreadNumMap.get(operatorID);
        long[] batchStartTimeArray = new long[threadNum];
        long[] batchEndTimeArray = new long[threadNum];
        double[] avgLatencyArray = new double[threadNum];
        double[] minLatencyArray = new double[threadNum];
        double[] maxLatencyArray = new double[threadNum];
        long[] batchSizeArray = new long[threadNum];
        long batchSizeSum = 0;

        int latestBatchID = opNumThreadCompletedMap.get(operatorID).keySet().stream().max(Integer::compare).orElseThrow(NullPointerException::new);

        for (int i=0; i<threadNum; i++) {
            batchStartTimeArray[i] = Objects.requireNonNull(opBatchStartTimeMap.get(operatorID).get(latestBatchID))[i];
            batchEndTimeArray[i] = Objects.requireNonNull(opBatchEndTimeMap.get(operatorID).get(latestBatchID))[i];
            DescriptiveStatistics[] batchLatencyStats = Objects.requireNonNull(opLatencyMap.get(operatorID).get(latestBatchID));
            avgLatencyArray[i] = batchLatencyStats[i].getMean();
            minLatencyArray[i] = batchLatencyStats[i].getMin();
            maxLatencyArray[i] = batchLatencyStats[i].getMax();
            batchSizeArray[i] = batchLatencyStats[i].getN();
            batchSizeSum += batchSizeArray[i];
        }

        long batchStartTime = Arrays.stream(batchStartTimeArray).min().orElse(Long.MAX_VALUE);
        long batchEndTime = Arrays.stream(batchEndTimeArray).max().orElse(Long.MIN_VALUE);
        long batchDuration = batchEndTime - batchStartTime;
        assert batchDuration > 0;
        double throughput = batchSizeSum * 1E9 / batchDuration;
        opThroughputMap.get(operatorID).put(latestBatchID, throughput); // keep record for throughput history

        double latencySum = 0;
        for (int i=0; i<threadNum; i++) {
            latencySum += avgLatencyArray[i] * batchSizeArray[i];
        }
        double avgLatency = latencySum / batchSizeSum;
        double minLatency = Arrays.stream(minLatencyArray).min().orElse(Double.MAX_VALUE);
        double maxLatency = Arrays.stream(maxLatencyArray).max().orElse(Double.MIN_VALUE);

        BatchRuntimeData batchRuntimeData = new BatchRuntimeData(applicationID, String.valueOf(operatorID),
                throughput, minLatency, maxLatency, avgLatency, batchSizeSum);
        webSocketHandler.getBatchInfoSender().send(batchRuntimeData.toString());
    }


    @Override
    public void run() {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(webSocketHandler);
            Channel channel = bootstrap.bind(5001).sync().channel();
            channel.closeFuture().sync(); // block until server is closed

            while (true) {
                try {
                    int operatorID = (int) readyOperatorQueue.take();
                    sendDataToFrontend(operatorID);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }



}

