package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.input.MessageBatch;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.ResultBatch;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaNode;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaCompletionListener;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaConnectionListener;
import intellistream.morphstream.common.io.Rdma.Memory.CircularRdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.RdmaBufferManager;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;
import intellistream.morphstream.configuration.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class RdmaWorkerManager implements Serializable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(RdmaWorkerManager.class);
    private final RdmaNode rdmaNode;
    private final int totalFunctionExecutors;
    private final boolean isDriver;
    private final int managerId;
    private final String[] workerHosts;
    private final String[] workerPorts;
    private final String driverHost;
    private final int driverPort;
    private final Configuration conf;
    private final RdmaBufferManager rdmaBufferManager;
    private RdmaChannel driverRdmaChannel;
    private RegionToken resultRegionToken;// Result region token to driver
    private ResultBatch resultBatch;// Results
    private ConcurrentHashMap<Integer, RdmaChannel> workerRdmaChannelMap = new ConcurrentHashMap<>();//workerId -> RdmaChannel
    private ConcurrentHashMap<Integer, RegionToken> workerRegionTokenMap = new ConcurrentHashMap<>();//workerId -> RegionToken

    public RdmaWorkerManager(boolean isDriver, Configuration conf) throws Exception {
        this.isDriver = isDriver;
        this.conf = conf;
        this.totalFunctionExecutors = MorphStreamEnv.get().configuration().getInt("tthread");
        workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        workerPorts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerPorts").split(",");
        managerId = MorphStreamEnv.get().configuration().getInt("workerId", 0);
        driverHost = MorphStreamEnv.get().configuration().getString("morphstream.rdma.driverHost");
        driverPort = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.driverPort");
        rdmaNode = new RdmaNode(workerHosts[managerId],  Integer.parseInt(workerPorts[managerId]), conf.rdmaChannelConf, conf.rdmaChannelConf.getRdmaChannelType());
        rdmaBufferManager = rdmaNode.getRdmaBufferManager();
        resultBatch = new ResultBatch(MorphStreamEnv.get().configuration().getInt("maxResultsCapacity"), MorphStreamEnv.get().configuration().getInt("frontendNum"), this.totalFunctionExecutors);
        //Result count to decide whether to send the batched results
        SOURCE_CONTROL.getInstance().config(MorphStreamEnv.get().configuration().getInt("frontendNum"), MorphStreamEnv.get().configuration().getInt("sendMessagePerFrontend"), this.totalFunctionExecutors, MorphStreamEnv.get().configuration().getInt("returnResultPerExecutor"));
        //Connect to driver
        driverRdmaChannel = rdmaNode.getRdmaChannel(new InetSocketAddress(driverHost, driverPort), true, conf.rdmaChannelConf.getRdmaChannelType());
        //Receive region token from driver
        resultRegionToken = rdmaNode.getRemoteRegionToken(driverRdmaChannel);
        rdmaBufferManager.perAllocateCircularRdmaBuffer(MorphStreamEnv.get().configuration().getInt("CircularBufferCapacity"), MorphStreamEnv.get().configuration().getInt("tthread"));
        //Send region token to driver
        rdmaNode.sendRegionTokenToRemote(driverRdmaChannel, rdmaBufferManager.getCircularRdmaBuffer().createRegionToken(), driverHost);
        //Wait for other workers to connect
        rdmaNode.bindConnectCompleteListener(new RdmaConnectionListener() {
            @Override
            public void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) {
                LOG.info("Worker accepts " + inetSocketAddress.toString());
                for (int i = 0; i < workerHosts.length; i++) {
                    if (workerHosts[i].equals(inetSocketAddress.getHostName()) && Integer.parseInt(workerPorts[i]) == inetSocketAddress.getPort()) {
                        workerRdmaChannelMap.put(i, rdmaChannel);
                    }
                }
            }
            @Override
            public void onFailure(Throwable exception) {
                LOG.warn("Worker fails to accept " + exception.getMessage());
            }
        });
        //Connect to other workers
        for (int i = managerId + 1; i < workerHosts.length; i++) {
            if (i != managerId) {
                workerRdmaChannelMap.put(i, rdmaNode.getRdmaChannel(new InetSocketAddress(workerHosts[i], Integer.parseInt(workerPorts[i])), true, conf.rdmaChannelConf.getRdmaChannelType()));
            }
        }
    }
    public CircularRdmaBuffer getCircularRdmaBuffer() {
        return rdmaBufferManager.getCircularRdmaBuffer();
    }

    public void send(int senderThreadId, FunctionMessage functionMessage) throws Exception {
        this.resultBatch.add(senderThreadId, functionMessage);
        if (this.resultBatch.getTotalResultCount(senderThreadId) >= SOURCE_CONTROL.getInstance().getResultPerExecutor()) {
            SOURCE_CONTROL.getInstance().workerStartSendResultBarrier();
            if (senderThreadId == 0)
                sendBatch();
            SOURCE_CONTROL.getInstance().workerEndSendResultBarrier();
        }
    }
    public void sendBatch() throws Exception {
        ByteBuffer byteBuffer = this.resultBatch.buffer();
        byteBuffer.flip();

        RdmaBuffer rdmaBuffer = rdmaBufferManager.get(byteBuffer.capacity());
        ByteBuffer dataBuffer = rdmaBuffer.getByteBuffer();
        dataBuffer.put(byteBuffer);
        dataBuffer.flip();

        RdmaChannel rdmaChannel = driverRdmaChannel;
        RegionToken regionToken = resultRegionToken;

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
                    resultBatch.clear();
                    latch.countDown();
                    LOG.info("Worker " + managerId + " sends results" + " to driver.");
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
        latch.await();
    }
}
