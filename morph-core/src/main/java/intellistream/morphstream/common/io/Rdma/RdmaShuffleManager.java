package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.launcher.MorphStreamEvn;
import intellistream.morphstream.common.io.Rdma.Msg.RdmaAnnounceRdmaShuffleManagersRpcMsg;
import intellistream.morphstream.common.io.Rdma.Msg.RdmaRpcMsg;
import intellistream.morphstream.common.io.Rdma.Msg.RdmaShuffleManagerHelloRpcMsg;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.BlockManagerId;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.RdmaShuffleManagerId;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Stats.RdmaShuffleReaderStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;

public class RdmaShuffleManager {
    private Logger LOG = LoggerFactory.getLogger(RdmaShuffleManager.class);
    public RdmaShuffleConf conf;
    private final boolean isDriver;
    private RdmaNode rdmaNode;
    private RdmaShuffleManagerId localRdmaShuffleManagerId;
    public RdmaShuffleBlockResolver shuffleBlockResolver = new RdmaShuffleBlockResolver();
    // Used by diver only
    private final Map<BlockManagerId, Map<Integer, Map<Integer, RdmaMapTaskOutput>>> mapTaskOutputsByBlockManagerId = new ConcurrentHashMap<>(); // blockManagerId -> shuffleId -> mapId -> mapTaskOutput
    private final Map<RdmaShuffleManagerId, RdmaChannel> rdmaShuffleManagersMap = new ConcurrentHashMap<>();// shuffleManagerId -> rdmaChannel
    private final Map<BlockManagerId, RdmaShuffleManagerId> blockManagerIdToRdmaShuffleManagerId = new ConcurrentHashMap<>(); // blockManagerId -> rdmaShuffleManagerId
    private final Map<Integer, RdmaBuffer> shuffleIdToBufferAddress = new ConcurrentHashMap<>(); // shuffleId -> shuffleBuffer
    // Used by executor only
    public RdmaShuffleReaderStats rdmaShuffleReaderStats;
    public RdmaShuffleManager(RdmaShuffleConf conf, boolean isDriver) throws Exception {
        this.conf = conf;
        this.isDriver = isDriver;
        if (conf.collectOdpStats) {
            rdmaShuffleReaderStats = new RdmaShuffleReaderStats(conf);
        }
        if (isDriver) {
            this.rdmaNode = new RdmaNode(conf.getConfKey("morphstream.driver.host"), false, conf, receiveListener);
            conf.setDriverPort(String.valueOf(rdmaNode.getLocalInetSocketAddress().getPort()));
        }
    }

    /**
     * Start the RDMA node for executor.
     */
    private void startRdmaNodeIfMissing() throws Exception {
        assert !isDriver;
        boolean shouldSendHelloMsg = false;
        synchronized (this) {
            if (localRdmaShuffleManagerId == null) {
                assert rdmaNode == null;
                shouldSendHelloMsg = true;
                rdmaNode = new RdmaNode(MorphStreamEvn.get().blockManagerId().getHost(), true, conf, receiveListener);
                localRdmaShuffleManagerId = new RdmaShuffleManagerId(rdmaNode.getLocalInetSocketAddress().getHostName(), rdmaNode.getLocalInetSocketAddress().getPort(), MorphStreamEvn.get().blockManagerId());
            }
        }

        assert rdmaNode != null;
        //Establish a connection to the driver in the background.
        if (shouldSendHelloMsg) {
            CompletableFuture.completedFuture(this.getRdmaChannelToDiver(true)).thenAccept(rdmaChannel -> {
                try {
                    int port = rdmaChannel.getSourceSocketAddress().getPort();
                    RdmaByteBufferManagedBuffer[] buffers = new RdmaShuffleManagerHelloRpcMsg(localRdmaShuffleManagerId, port).toRdmaByteBufferManagedBuffers(getRdmaByteBufferManagedBuffer, conf.recvWrSize);
                    RdmaCompletionListener listener = new RdmaCompletionListener() {
                        @Override
                        public void onSuccess(ByteBuffer buffer) {
                            for (RdmaByteBufferManagedBuffer buf: buffers) {
                                buf.release();
                            }
                        }
                        @Override
                        public void onFailure(Throwable exception) {
                            for (RdmaByteBufferManagedBuffer buf: buffers) {
                                buf.release();
                            }
                            LOG.error("Failed to send RdmaExecutorHelloRpcMsg to driver", exception);
                        }
                    };
                    long[] addresses = Arrays.stream(buffers).mapToLong(RdmaByteBufferManagedBuffer::getAddress).toArray();
                    int[] lKeys = Arrays.stream(buffers).mapToInt(RdmaByteBufferManagedBuffer::getLkey).toArray();
                    long[] sizes = Arrays.stream(buffers).mapToLong(RdmaByteBufferManagedBuffer::getLength).toArray();
                    rdmaChannel.rdmaSendInQueue(listener, addresses, lKeys, sizes);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            //Pre allocate buffers in parallel outside of synchronization block.
            conf.preAllocateBuffers.forEach((buffSize, buffCount) -> {
                try {
                    getRdmaBufferManager().preAllocate(buffSize, buffCount);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
    public RdmaChannel getRdmaChannelToDiver(boolean mustRetry) throws IOException, InterruptedException {
        return getRdmaChannel(conf.driverHost, conf.driverPort, mustRetry, RdmaChannel.RdmaChannelType.RPC);
    }
    public RdmaChannel getRdmaChannelOfREAD_REQUESTOR(RdmaShuffleManagerId rdmaShuffleManagerId, boolean mustRetry) throws IOException, InterruptedException {
        return getRdmaChannel(rdmaShuffleManagerId.getBlockManagerId().getHost(), rdmaShuffleManagerId.getBlockManagerId().getPort(), mustRetry, RdmaChannel.RdmaChannelType.RDMA_READ_REQUESTOR);
    }
    private RdmaChannel getRdmaChannel(String host, int port, boolean mustRetry, RdmaChannel.RdmaChannelType rdmaChannelType) throws IOException, InterruptedException {
         return this.rdmaNode.getRdmaChannel(new InetSocketAddress(host, port), mustRetry, rdmaChannelType);
    }
    IntFunction<RdmaByteBufferManagedBuffer> getRdmaByteBufferManagedBuffer = length -> {
        try {
            return new RdmaByteBufferManagedBuffer(new RdmaRegisteredBuffer(this.getRdmaBufferManager(),length),length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };
    public RdmaBufferManager getRdmaBufferManager() {
        return this.rdmaNode.getRdmaBufferManager();
    }
    //Shared implementation for receive RPC handling for both driver and executor.
    RdmaCompletionListener receiveListener = new RdmaCompletionListener() {
        @Override
        public void onSuccess(ByteBuffer buffer) {
            try {
                RdmaRpcMsg rpcMsg = RdmaRpcMsg.apply(buffer);
                if (rpcMsg instanceof RdmaShuffleManagerHelloRpcMsg){
                    // Each executor advertises itself to the driver, so the driver can announce all
                    // executor RDMA addresses to all other executors. This is used for establishing RDMA
                    // connections in the background, so connections will be ready when shuffle phases start
                    assert isDriver;
                    RdmaShuffleManagerHelloRpcMsg helloMsg = (RdmaShuffleManagerHelloRpcMsg) rpcMsg;
                    if (!rdmaShuffleManagersMap.containsKey(helloMsg.rdmaShuffleManagerId)) {
                        //Keep mapping from BlockManagerId to RdmaShuffleManagerId
                        blockManagerIdToRdmaShuffleManagerId.put(helloMsg.rdmaShuffleManagerId.getBlockManagerId(), helloMsg.rdmaShuffleManagerId);
                        // Since we're reusing executor <-> driver QP - whis will be taken from cache.
                        RdmaChannel rdmaChannel = getRdmaChannel(helloMsg.rdmaShuffleManagerId.getBlockManagerId().getHost(), helloMsg.channelPort, false, RdmaChannel.RdmaChannelType.RPC);
                        rdmaShuffleManagersMap.put(helloMsg.rdmaShuffleManagerId, rdmaChannel);
                        RdmaAnnounceRdmaShuffleManagersRpcMsg announceMsg = new RdmaAnnounceRdmaShuffleManagersRpcMsg(new ArrayList<>(rdmaShuffleManagersMap.keySet()));
                        RdmaByteBufferManagedBuffer[] buffers = announceMsg.toRdmaByteBufferManagedBuffers(getRdmaByteBufferManagedBuffer, conf.recvWrSize);
                        //Send a list of known executor RDMA addresses to executor
                        for (Map.Entry<RdmaShuffleManagerId, RdmaChannel> entry : rdmaShuffleManagersMap.entrySet()) {
                            for (RdmaByteBufferManagedBuffer buf : buffers) {
                                buf.retain();
                            }
                            RdmaCompletionListener listener = new RdmaCompletionListener() {
                                @Override
                                public void onSuccess(ByteBuffer buf) {
                                    for (RdmaByteBufferManagedBuffer buffer : buffers) {
                                        buffer.release();
                                    }
                                }
                                @Override
                                public void onFailure(Throwable e) {
                                    for (RdmaByteBufferManagedBuffer buffer : buffers) {
                                        buffer.release();
                                    }
                                    LOG.error("Failed to send RdmaAnnounceRdmaShuffleManagersRpcMsg to executor: " + entry.getKey(), e);
                                }
                            };
                            long[] addresses = Arrays.stream(buffers).mapToLong(RdmaByteBufferManagedBuffer::getAddress).toArray();
                            int[] lKeys = Arrays.stream(buffers).mapToInt(RdmaByteBufferManagedBuffer::getLkey).toArray();
                            long[] sizes = Arrays.stream(buffers).mapToLong(RdmaByteBufferManagedBuffer::getLength).toArray();
                            entry.getValue().rdmaSendInQueue(listener, addresses, lKeys, sizes);
                        }
                        // Release the reference taken by the allocation
                        for (RdmaByteBufferManagedBuffer buf : buffers) {
                            buf.release();
                        }
                    }
                } else if (rpcMsg instanceof RdmaAnnounceRdmaShuffleManagersRpcMsg) {
                    // Driver advertises a list of known executor RDMA addresses so connection establishment
                    // can be done in the background, before shuffle phases begin
                    assert !isDriver;
                    RdmaAnnounceRdmaShuffleManagersRpcMsg announceMsg = (RdmaAnnounceRdmaShuffleManagersRpcMsg) rpcMsg;
                    List<RdmaShuffleManagerId> shuffleManagerIds = announceMsg.rdmaShuffleManagerIds;
                    shuffleManagerIds.remove(localRdmaShuffleManagerId);
                    for (RdmaShuffleManagerId rdmaShuffleManagerId : shuffleManagerIds) {
                        blockManagerIdToRdmaShuffleManagerId.put(rdmaShuffleManagerId.getBlockManagerId(), rdmaShuffleManagerId);
                        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                            try {
                                getRdmaChannelOfREAD_REQUESTOR(rdmaShuffleManagerId, false);
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                } else {
                    LOG.warn("Receive RdmaCompletionListener encountered an unidentified RPC message type");
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onFailure(Throwable exception) {
            LOG.error("Exception in Receive RdmaCompletionListener (ignoring): ", exception);
        }
    };

}
