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
    private final RdmaBufferManager rdmaBufferManager;
    private ConcurrentHashMap<Integer, RdmaChannel> workerRdmaChannelMap = new ConcurrentHashMap<>();//workerId -> RdmaChannel
    private ConcurrentHashMap<Integer, RegionToken> workerRegionTokenMap = new ConcurrentHashMap<>();//workerId -> RegionToken
    @Getter
    private ConcurrentHashMap<Integer, MessageBatch> workerMessageBatchMap = new ConcurrentHashMap<>();//workerId -> MessageBatch
    public RdmaDriverManager(boolean isDriver, Configuration conf) throws Exception {
        this.isDriver = isDriver;
        this.conf = conf;
        workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        workerPorts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerPorts").split(",");
        driverHost = MorphStreamEnv.get().configuration().getString("morphstream.rdma.driverHost");
        driverPort = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.driverPort");
        rdmaNode = new RdmaNode(driverHost, driverPort, conf.rdmaChannelConf, conf.rdmaChannelConf.getRdmaChannelType());
        rdmaBufferManager = rdmaNode.getRdmaBufferManager();
        workerLatch = MorphStreamEnv.get().workerLatch();
        //PreAllocate CircularRdmaBuffer to receive results from workers
        rdmaBufferManager.perAllocateResultBuffer(MorphStreamEnv.get().configuration().getInt("workerNum"), MorphStreamEnv.get().configuration().getInt("CircularBufferCapacity"), MorphStreamEnv.get().configuration().getInt("frontendNum"));
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
                        workerMessageBatchMap.put(i, new MessageBatch(MorphStreamEnv.get().configuration().getInt("BatchMessageCapacity"), MorphStreamEnv.get().configuration().getInt("tthread")));
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
    public void send(int workId, FunctionMessage functionMessage) throws Exception {
        synchronized (workerMessageBatchMap.get(workId).getWriteLock()) {
            workerMessageBatchMap.get(workId).add(functionMessage);
            if (workerMessageBatchMap.get(workId).isFull()) {
                sendBatch(workId);
            }
        }
    }
    public void sendBatch(int workId) throws Exception {
        synchronized (workerMessageBatchMap.get(workId).getWriteLock()) {
            if (workerMessageBatchMap.get(workId).isEmpty()) {
                return;
            }
            RdmaChannel rdmaChannel = workerRdmaChannelMap.get(workId);
            RegionToken regionToken = workerRegionTokenMap.get(workId);
            MessageBatch messageBatch = workerMessageBatchMap.get(workId);
            ByteBuffer byteBuffer = messageBatch.buffer();
            RdmaBuffer rdmaBuffer = rdmaBufferManager.get(byteBuffer.capacity());
            rdmaBuffer.getByteBuffer().put(byteBuffer);

            long remoteAddress = regionToken.getAddress();
            int rkey = regionToken.getRemoteKey();
            int sizeInBytes = regionToken.getSizeInBytes();

            rdmaBuffer.getByteBuffer().flip();
            rdmaChannel.rdmaWriteInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buffer, Integer imm) {
                    try {
                        rdmaBuffer.getByteBuffer().clear();
                        rdmaBufferManager.put(rdmaBuffer);
                        regionToken.setAddress(remoteAddress + byteBuffer.capacity());
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
            }, rdmaBuffer.getAddress(), sizeInBytes, rdmaBuffer.getLkey(), remoteAddress, rkey);
        }
    }
    public RdmaBufferManager getRdmaBufferManager() {
        return rdmaBufferManager;
    }
}
