package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.input.MessageBatch;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaNode;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaCompletionListener;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaConnectionListener;
import intellistream.morphstream.common.io.Rdma.Memory.CircularRdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.RdmaBufferManager;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.configuration.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

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
    private ConcurrentHashMap<Integer, RdmaChannel> workerRdmaChannelMap = new ConcurrentHashMap<>();//workerId -> RdmaChannel
    private ConcurrentHashMap<Integer, RegionToken> workerRegionTokenMap = new ConcurrentHashMap<>();//workerId -> RegionToken
    private MessageBatch messageBatch;// Results
    private RegionToken resultRegionToken;
    private RdmaChannel driverRdmaChannel;
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
        messageBatch = new MessageBatch(MorphStreamEnv.get().configuration().getInt("BatchMessageCapacity"), MorphStreamEnv.get().configuration().getInt("frontendNum"));
        //Connect to driver
        driverRdmaChannel = rdmaNode.getRdmaChannel(new InetSocketAddress(driverHost, driverPort), true, conf.rdmaChannelConf.getRdmaChannelType());
        //Receive region token from driver
        resultRegionToken = rdmaNode.getRemoteRegionToken(driverRdmaChannel);
        rdmaBufferManager.perAllocateCircularRdmaBuffer(MorphStreamEnv.get().configuration().getInt("CircularBufferCapacity"), MorphStreamEnv.get().configuration().getInt("tthread"));
        //Send region token to driver
        rdmaNode.sendRegionTokenToRemote(driverRdmaChannel, rdmaBufferManager.getCircularRdmaBuffer().createRegionToken(), driverHost);
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

    public void send(FunctionMessage functionMessage) throws Exception {
        synchronized (messageBatch.getWriteLock()) {
            messageBatch.add(functionMessage);
            if (messageBatch.isFull()) {
                sendBatch();
            }
        }
    }
    public void sendBatch() throws Exception {
        synchronized (messageBatch.getWriteLock()) {
            if (messageBatch.isEmpty()) {
                return;
            }
            RdmaChannel rdmaChannel = driverRdmaChannel;
            RegionToken regionToken = resultRegionToken;
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
}
