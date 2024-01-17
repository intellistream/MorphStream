package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.input.MessageBatch;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaNode;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaCompletionListener;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaConnectionListener;
import intellistream.morphstream.common.io.Rdma.Memory.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.RdmaBufferManager;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;
import intellistream.morphstream.configuration.Configuration;
import lombok.Getter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class RdmaDriverManager {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(RdmaDriverManager.class);
    private final RdmaNode rdmaNode;
    private final boolean isDriver;
    private final String[] workerHosts;
    private final String[] workerPorts;
    private final String driverHost;
    private final int driverPort;
    private final Configuration conf;
    private final CountDownLatch workerLatch;
    @Getter
    private final RdmaBufferManager rdmaBufferManager;
    private ConcurrentHashMap<Integer, RdmaChannel> workerRdmaChannelMap = new ConcurrentHashMap<>();//workerId -> RdmaChannel
    private ConcurrentHashMap<Integer, RegionToken> workerRegionTokenMap = new ConcurrentHashMap<>();//workerId -> RegionToken
    @Getter
    private ConcurrentHashMap<Integer, MessageBatch> workerMessageBatchMap = new ConcurrentHashMap<>();//workerId -> MessageBatch
    private ConcurrentHashMap<Integer, Integer> frontendTotalMessageCountMap = new ConcurrentHashMap<>();//frontendId -> total message count
    public RdmaDriverManager(boolean isDriver, Configuration conf) throws Exception {
        this.isDriver = isDriver;
        this.conf = conf;
        workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        workerPorts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerPorts").split(",");
        driverHost = MorphStreamEnv.get().configuration().getString("morphstream.rdma.driverHost");
        driverPort = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.driverPort");
        rdmaNode = new RdmaNode(driverHost, driverPort, conf.rdmaChannelConf, RdmaChannel.RdmaChannelType.RDMA_WRITE_REQUESTOR);
        rdmaBufferManager = rdmaNode.getRdmaBufferManager();
        workerLatch = MorphStreamEnv.get().workerLatch();
        //PreAllocate CircularRdmaBuffer to receive results from workers
        rdmaBufferManager.perAllocateResultBuffer(MorphStreamEnv.get().configuration().getInt("workerNum"), MorphStreamEnv.get().configuration().getInt("CircularBufferCapacity"), MorphStreamEnv.get().configuration().getInt("frontendNum"));
        //Message count to decide whether to send the batched messages
        SOURCE_CONTROL.getInstance().config(MorphStreamEnv.get().configuration().getInt("frontendNum"), MorphStreamEnv.get().configuration().getInt("sendMessagePerFrontend"), MorphStreamEnv.get().configuration().getInt("tthread"), MorphStreamEnv.get().configuration().getInt("returnResultPerExecutor"));
        for (int i = 0; i < MorphStreamEnv.get().configuration().getInt("frontendNum"); i++) {
            frontendTotalMessageCountMap.put(i, 0);
        }
        //Wait for workers to connect
        rdmaNode.bindConnectCompleteListener(new RdmaConnectionListener() {
            @Override
            public void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) throws Exception {
                LOG.info("Driver accepts " + inetSocketAddress.toString());
                for (int i = 0; i < workerHosts.length; i++) {
                    if (workerHosts[i].equals(inetSocketAddress.getHostName())) {
                        //Send region token to worker
                        rdmaNode.sendRegionTokenToRemote(rdmaChannel, rdmaBufferManager.getResultBuffer(i).createRegionToken(), inetSocketAddress.getHostName());
                        //Receive region token from worker
                        RegionToken regionToken = rdmaNode.getRemoteRegionToken(rdmaChannel);
                        workerRdmaChannelMap.put(i, rdmaChannel);
                        workerRegionTokenMap.put(i, regionToken);
                        workerMessageBatchMap.put(i, new MessageBatch(MorphStreamEnv.get().configuration().getInt("maxMessageCapacity"), MorphStreamEnv.get().configuration().getInt("tthread")));
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
        synchronized (workerMessageBatchMap.get(workId).getWriteLock()) {
            workerMessageBatchMap.get(workId).add(functionMessage);
            frontendTotalMessageCountMap.put(frontendId, frontendTotalMessageCountMap.get(frontendId) + 1);
        }
        if (frontendTotalMessageCountMap.get(frontendId) >= SOURCE_CONTROL.getInstance().getMessagePerFrontend()) {
            SOURCE_CONTROL.getInstance().driverStartSendMessageBarrier();
            sendBatch(frontendId);
            SOURCE_CONTROL.getInstance().driverEndSendMessageBarrier();
        }
    }
    public void sendBatch(int workId) throws Exception {
        if (workerMessageBatchMap.get(workId) == null || workerMessageBatchMap.get(workId).isEmpty()) {
            frontendTotalMessageCountMap.put(workId, 0);
            return;
        }
        int totalMessageCount = workerMessageBatchMap.get(workId).size();
        MessageBatch messageBatch = workerMessageBatchMap.get(workId);
        ByteBuffer byteBuffer = messageBatch.buffer();
        byteBuffer.flip();

        RdmaBuffer rdmaBuffer = rdmaBufferManager.get(byteBuffer.capacity());
        ByteBuffer dataBuffer = rdmaBuffer.getByteBuffer();
        dataBuffer.put(byteBuffer);
        dataBuffer.flip();

        RdmaChannel rdmaChannel = workerRdmaChannelMap.get(workId);
        RegionToken regionToken = workerRegionTokenMap.get(workId);

        long remoteAddress = regionToken.getAddress();
        int rkey = regionToken.getLocalKey();
        CountDownLatch latch = new CountDownLatch(1);
        rdmaChannel.rdmaWriteInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer, Integer imm) {
                try {
                    rdmaBuffer.getByteBuffer().clear();
                    rdmaBufferManager.put(rdmaBuffer);
                    regionToken.setAddress(remoteAddress + byteBuffer.capacity());
                    frontendTotalMessageCountMap.put(workId, 0);
                    messageBatch.clear();
                    latch.countDown();
                    LOG.info("Driver sends " + totalMessageCount + " to worker " + workId);
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
}
