package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.input.MessageBatch;
import intellistream.morphstream.api.input.statistic.DriverSideOwnershipTable;
import intellistream.morphstream.api.input.statistic.Statistic;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaNode;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaCompletionListener;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaConnectionListener;
import intellistream.morphstream.common.io.Rdma.Memory.Manager.DriverRdmaBufferManager;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.DWRegionTokenGroup;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.common.io.Rdma.Msg.WDRegionTokenGroup;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import lombok.Getter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL.*;

public class RdmaDriverManager {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(RdmaDriverManager.class);
    private final RdmaNode rdmaNode;
    private final boolean isDriver;
    private final Statistic statistic;
    private final int punctuation_interval;
    private final String[] workerHosts;
    private final String[] workerPorts;
    private final String[] tableNames;
    private final String driverHost;
    private final int driverPort;
    private final Configuration conf;
    private final CountDownLatch workerLatch;
    @Getter
    private final DriverRdmaBufferManager rdmaBufferManager;
    private ConcurrentHashMap<Integer, RdmaChannel> workerRdmaChannelMap = new ConcurrentHashMap<>();//workerId -> RdmaChannel
    private ConcurrentHashMap<Integer, WDRegionTokenGroup> workerRegionTokenMap = new ConcurrentHashMap<>();//workerId -> RegionToken
    @Getter
    private ConcurrentHashMap<Integer, MessageBatch> workerMessageBatchMap = new ConcurrentHashMap<>();//workerId -> MessageBatch
    private ConcurrentHashMap<Integer, Integer> frontendTotalMessageCountMap = new ConcurrentHashMap<>();//frontendId -> total message count
    private ConcurrentHashMap<Integer, Integer> frontendTotalBatchCountMap = new ConcurrentHashMap<>();//frontendId -> total batch count
    public RdmaDriverManager(boolean isDriver, Configuration conf, Statistic statistic) throws Exception {
        this.isDriver = isDriver;
        this.conf = conf;
        this.statistic = statistic;
        this.punctuation_interval = MorphStreamEnv.get().configuration().getInt("totalBatch");
        workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        workerPorts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerPorts").split(",");
        driverHost = MorphStreamEnv.get().configuration().getString("morphstream.rdma.driverHost");
        driverPort = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.driverPort");
        tableNames = MorphStreamEnv.get().configuration().getString("tableNames").split(";");
        rdmaNode = new RdmaNode(driverHost, driverPort, conf.rdmaChannelConf, RdmaChannel.RdmaChannelType.RDMA_WRITE_REQUESTOR, isDriver, false);
        rdmaBufferManager = (DriverRdmaBufferManager) rdmaNode.getRdmaBufferManager();
        workerLatch = MorphStreamEnv.get().workerLatch();
        //PreAllocate CircularRdmaBuffer to receive results from workers
        rdmaBufferManager.perAllocateResultBuffer(MorphStreamEnv.get().configuration().getInt("workerNum"), MorphStreamEnv.get().configuration().getInt("CircularBufferCapacity"), MorphStreamEnv.get().configuration().getInt("frontendNum"));
        //Message count to decide whether to send the batched messages
        SOURCE_CONTROL.getInstance().config(MorphStreamEnv.get().configuration().getInt("frontendNum"), MorphStreamEnv.get().configuration().getInt("sendMessagePerFrontend"), MorphStreamEnv.get().configuration().getInt("tthread"), MorphStreamEnv.get().configuration().getInt("returnResultPerExecutor"));
        for (int i = 0; i < MorphStreamEnv.get().configuration().getInt("frontendNum"); i++) {
            frontendTotalMessageCountMap.put(i, 0);
            frontendTotalBatchCountMap.put(i, 0);
        }
        //Wait for workers to connect
        rdmaNode.bindConnectCompleteListener(new RdmaConnectionListener() {
            @Override
            public void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) throws Exception {
                LOG.info("Driver accepts " + inetSocketAddress.toString());
                for (int i = 0; i < workerHosts.length; i++) {
                    if (workerHosts[i].equals(inetSocketAddress.getHostName())) {
                        //Send region token to worker
                        DWRegionTokenGroup dwRegionTokenGroup = new DWRegionTokenGroup();
                        dwRegionTokenGroup.addRegionToken(rdmaBufferManager.getResultBuffer(i).createRegionToken());
                        rdmaNode.sendRegionTokenToRemote(rdmaChannel, dwRegionTokenGroup.getRegionTokens(), inetSocketAddress.getHostName());
                        //Receive region token from worker
                        WDRegionTokenGroup wdRegionTokenGroup = new WDRegionTokenGroup();
                        wdRegionTokenGroup.addRegionTokens(rdmaNode.getRemoteRegionToken(rdmaChannel));
                        workerRdmaChannelMap.put(i, rdmaChannel);
                        workerRegionTokenMap.put(i, wdRegionTokenGroup);
                        workerMessageBatchMap.put(i, new MessageBatch(MorphStreamEnv.get().configuration().getInt("maxMessageCapacity"), MorphStreamEnv.get().configuration().getInt("tthread"), MorphStreamEnv.get().configuration().getInt("frontendNum")));
                    }
                }
                workerLatch.countDown();
            }
            @Override
            public void onFailure(Throwable exception) {
                LOG.warn("Driver fails to accept " + exception.getMessage());
            }
        });
    }
    public void send(int frontendId, int workId, FunctionMessage functionMessage) throws Exception {
        workerMessageBatchMap.get(workId).add(frontendId, functionMessage);
        frontendTotalMessageCountMap.put(frontendId, frontendTotalMessageCountMap.get(frontendId) + 1);
        MeasureTools.DriverPrepareEndTime(frontendId);
        if (frontendTotalMessageCountMap.get(frontendId) >= SOURCE_CONTROL.getInstance().getMessagePerFrontend()) {
            SOURCE_CONTROL.getInstance().driverStartSendMessageBarrier();
            MeasureTools.DriverRdmaStartSendEventTime(frontendId);
            sendBatch(frontendId);
            frontendTotalBatchCountMap.put(frontendId, frontendTotalBatchCountMap.get(frontendId) + 1);
            if (model_switch(frontendId)) {
                sendOwnershipTable(frontendId);
                SOURCE_CONTROL.getInstance().driverEndSendMessageBarrier();
                this.statistic.clear();
            }
            SOURCE_CONTROL.getInstance().driverEndSendMessageBarrier();
            MeasureTools.DriverRdmaEndSendEventTime(frontendId);
        }
    }
    private void sendBatch(int workId) throws Exception {
        if (workerMessageBatchMap.get(workId) == null || workerMessageBatchMap.get(workId).isEmpty()) {
            frontendTotalMessageCountMap.put(workId, 0);
            return;
        }
        MessageBatch messageBatch = workerMessageBatchMap.get(workId);
        ByteBuffer byteBuffer = messageBatch.buffer();
        byteBuffer.flip();

        RdmaBuffer rdmaBuffer = rdmaBufferManager.get(byteBuffer.capacity());
        ByteBuffer dataBuffer = rdmaBuffer.getByteBuffer();
        dataBuffer.put(byteBuffer);
        dataBuffer.flip();

        RdmaChannel rdmaChannel = workerRdmaChannelMap.get(workId);
        RegionToken regionToken = workerRegionTokenMap.get(workId).getCircularMessageToken();

        long remoteAddress = regionToken.getAddress();
        int rkey = regionToken.getLocalKey();

        regionToken.setAddress(remoteAddress + byteBuffer.capacity());
        frontendTotalMessageCountMap.put(workId, 0);
        messageBatch.clear();
        rdmaChannel.rdmaWriteInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer, Integer imm) {
                try {
                    rdmaBuffer.getByteBuffer().clear();
                    rdmaBufferManager.put(rdmaBuffer);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onFailure(Throwable exception) {
                try {
                    rdmaBuffer.getByteBuffer().clear();
                    rdmaBufferManager.put(rdmaBuffer);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, rdmaBuffer.getAddress(), rdmaBuffer.getLength(), rdmaBuffer.getLkey(), remoteAddress, rkey);
    }
    public void sendFinish(int workId) throws Exception {
        if (workerRdmaChannelMap.get(workId) == null) {
            return;
        }
        int tthread = MorphStreamEnv.get().configuration().getInt("tthread");
        RdmaBuffer writeData = rdmaBufferManager.get(8 + 4 * tthread);
        ByteBuffer dataBuffer = writeData.getByteBuffer();

        dataBuffer.putShort(START_FLAG);//2
        dataBuffer.putInt(0);//4
        for (int i = 0; i < tthread; i++) {
            dataBuffer.putInt(0);//4 * tthread
        }
        dataBuffer.putShort(END_FLAG);//2
        dataBuffer.flip();

        RdmaChannel rdmaChannel = workerRdmaChannelMap.get(workId);
        RegionToken regionToken = workerRegionTokenMap.get(workId).getCircularMessageToken();

        long remoteAddress = regionToken.getAddress();
        int rkey = regionToken.getLocalKey();
        CountDownLatch latch = new CountDownLatch(1);
        rdmaChannel.rdmaWriteInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer, Integer imm) {
                try {
                    writeData.getByteBuffer().clear();
                    rdmaBufferManager.put(writeData);
                    regionToken.setAddress(remoteAddress + dataBuffer.capacity());
                    latch.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onFailure(Throwable exception) {
                try {
                    writeData.getByteBuffer().clear();
                    rdmaBufferManager.put(writeData);
                    latch.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, writeData.getAddress(), writeData.getLength(), writeData.getLkey(), remoteAddress, rkey);
        latch.await();
    }
    private void sendOwnershipTable(int workId) throws Exception {
        if (workerRdmaChannelMap.get(workId) == null) {
            return;
        }
        int rdmaBufferCapacity = 0;
        List<ByteBuffer> byteBuffers = new ArrayList<>();

        for (String tableName : tableNames) {
            DriverSideOwnershipTable driverSideOwnershipTable = this.statistic.getOwnershipTable(tableName);
            ByteBuffer buffer = driverSideOwnershipTable.buffer();
            buffer.flip();
            byteBuffers.add(buffer);
            rdmaBufferCapacity += buffer.capacity();
        }

        RdmaBuffer rdmaBuffer = rdmaBufferManager.get(rdmaBufferCapacity);
        ByteBuffer dataBuffer = rdmaBuffer.getByteBuffer();

        for (ByteBuffer byteBuffer : byteBuffers) {
            dataBuffer.put(byteBuffer);
        }

        dataBuffer.flip();

        RdmaChannel rdmaChannel = workerRdmaChannelMap.get(workId);
        RegionToken regionToken = workerRegionTokenMap.get(workId).getTableToken();

        long remoteAddress = regionToken.getAddress();
        int rkey = regionToken.getLocalKey();
        CountDownLatch latch = new CountDownLatch(1);
        int finalRdmaBufferCapacity = rdmaBufferCapacity;
        rdmaChannel.rdmaWriteInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer, Integer imm) {
                try {
                    rdmaBuffer.getByteBuffer().clear();
                    rdmaBufferManager.put(rdmaBuffer);
                    regionToken.setAddress(remoteAddress + finalRdmaBufferCapacity);
                    latch.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onFailure(Throwable exception) {
                try {
                    rdmaBuffer.getByteBuffer().clear();
                    rdmaBufferManager.put(rdmaBuffer);
                    latch.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, rdmaBuffer.getAddress(), rdmaBuffer.getLength(), rdmaBuffer.getLkey(), remoteAddress, rkey);
        latch.await();
    }
    private boolean model_switch(int frontend_Id) {
        return frontendTotalBatchCountMap.get(frontend_Id) % punctuation_interval == 0;
    }

    public void close() {
        try {
            rdmaNode.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
