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
    private static final ConcurrentHashMap<Integer, LinkedList<DescriptiveStatistics[]>> opLatencyMap = new ConcurrentHashMap<>(); //operatorID -> latency[batchID][threadID]
    private static final ConcurrentHashMap<Integer, LinkedList<Double>> opThroughputMap = new ConcurrentHashMap<>(); //operatorID -> throughput[batchID]
    private static final ConcurrentHashMap<Integer, LinkedList<Long[]>> opBatchStartTimeMap = new ConcurrentHashMap<>(); //operatorID -> batchStartTime[batchID][threadID]
    private static final ConcurrentHashMap<Integer, LinkedList<Long[]>> opBatchEndTimeMap = new ConcurrentHashMap<>(); //operatorID -> batchEndTime[batchID][threadID]
    private static final ConcurrentHashMap<Integer, AtomicInteger> opNumThreadCompletedMap = new ConcurrentHashMap<>(); //operatorID -> num of threads that have submitted performance data
    private static final String[] operatorIDs = MorphStreamEnv.get().configuration().getString("operatorIDs").split(",");
    private static final String applicationID = MorphStreamEnv.get().configuration().getString("application");
    private static final HashMap<Integer, Integer> operatorThreadNumMap = new HashMap<>(); //operatorID -> its thread number
    private static final BlockingQueue<Object> readyOperatorQueue = new LinkedBlockingQueue<>(); //ID of operators whose performance data is ready to be shown in the UI
    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup(2);
    WebSocketHandler webSocketHandler = new WebSocketHandler();


    public static void Initialize() {
        for (String operatorIDString : operatorIDs) {
            int operatorID = Integer.parseInt(operatorIDString);
            operatorThreadNumMap.put(operatorID, MorphStreamEnv.get().configuration().getInt("threadNumOf_" + operatorID, 4));
            opLatencyMap.put(operatorID, new LinkedList<>());
            opThroughputMap.put(operatorID, new LinkedList<>());
            opBatchStartTimeMap.put(operatorID, new LinkedList<>());
            opBatchEndTimeMap.put(operatorID, new LinkedList<>());
            opNumThreadCompletedMap.put(operatorID, new AtomicInteger(0));
        }

        runtimeMonitor.start();
    }

    public void submitRuntimeData(int batchID, int operatorID, int threadID, DescriptiveStatistics latencyStats, long batchStartTime, long batchEndTime) {
        LOG.info("Batch " + batchID + " runtime data received from operator " + operatorID + " thread " + threadID);

        int threadNum = operatorThreadNumMap.get(operatorID);
        LinkedList<DescriptiveStatistics[]> batchLatencyStats = opLatencyMap.get(operatorID);
        LinkedList<Long[]> batchStartTimeStats = opBatchStartTimeMap.get(operatorID);
        LinkedList<Long[]> batchEndTimeStats = opBatchEndTimeMap.get(operatorID);

        if (batchLatencyStats.size() == batchID + 1) { // runtime data belongs to a new batch
            // latency
            DescriptiveStatistics[] newBatchLatency = new DescriptiveStatistics[threadNum];
            newBatchLatency[threadID] = latencyStats;
            batchLatencyStats.add(newBatchLatency);
            // batch start time
            Long[] newBatchStartTime = new Long[threadNum];
            newBatchStartTime[threadID] = batchStartTime;
            batchStartTimeStats.add(newBatchStartTime);
            // batch end time
            Long[] newBatchEndTime = new Long[threadNum];
            newBatchEndTime[threadID] = batchEndTime;
            batchEndTimeStats.add(newBatchEndTime);

        } else if (batchLatencyStats.size() == batchID) { // runtime data belongs to an existing batch
            batchLatencyStats.get(batchID-1)[threadID] = latencyStats;
            batchStartTimeStats.get(batchID-1)[threadID] = batchStartTime;
            batchEndTimeStats.get(batchID-1)[threadID] = batchEndTime;
        } else {
            throw new RuntimeException("Unexpected batchID");
        }

        if (opNumThreadCompletedMap.get(operatorID).incrementAndGet() == threadNum) {
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

        for (int i=0; i<threadNum; i++) {
            batchStartTimeArray[i] = Objects.requireNonNull(opBatchStartTimeMap.get(operatorID).peekLast())[i];
            batchEndTimeArray[i] = Objects.requireNonNull(opBatchEndTimeMap.get(operatorID).peekLast())[i];
            DescriptiveStatistics[] batchLatencyStats = Objects.requireNonNull(opLatencyMap.get(operatorID).peekLast());
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
        opThroughputMap.get(operatorID).add(throughput); // keep record for throughput history

        double latencySum = 0;
        for (int i=0; i<threadNum; i++) {
            latencySum += avgLatencyArray[i] * batchSizeArray[i];
        }
        double avgLatency = latencySum / batchSizeSum;
        double minLatency = Arrays.stream(minLatencyArray).min().orElse(Double.MAX_VALUE);
        double maxLatency = Arrays.stream(maxLatencyArray).max().orElse(Double.MIN_VALUE);

        BatchRuntimeData batchRuntimeData = new BatchRuntimeData();
        batchRuntimeData.setAppId(applicationID);
        batchRuntimeData.setOperatorID(String.valueOf(operatorID));
        batchRuntimeData.setThroughput(throughput);
        batchRuntimeData.setAvgLatency(avgLatency);
        batchRuntimeData.setMinLatency(minLatency);
        batchRuntimeData.setMaxLatency(maxLatency);
        batchRuntimeData.setBatchSize(batchSizeSum);

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

    public static RuntimeMonitor get() {
        return runtimeMonitor;
    }

}

