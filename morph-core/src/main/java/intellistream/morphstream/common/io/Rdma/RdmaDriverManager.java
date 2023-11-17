package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Msg.RdmaAnnounceRdmaShuffleManagersRpcMsg;
import intellistream.morphstream.common.io.Rdma.Msg.RdmaRpcMsg;
import intellistream.morphstream.common.io.Rdma.Msg.RdmaShuffleManagerHelloRpcMsg;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Block.BlockManagerId;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Block.RdmaShuffleManagerId;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Stats.RdmaShuffleReaderStats;
import intellistream.morphstream.common.io.Rdma.Shuffle.Handle.RdmaBaseShuffleHandle;
import intellistream.morphstream.common.io.Rdma.Shuffle.Handle.ShuffleHandle;
import intellistream.morphstream.common.io.Rdma.Shuffle.RW.ShuffleReader;
import intellistream.morphstream.common.io.Rdma.Shuffle.RW.ShuffleWriter;
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
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.IntFunction;

public class RdmaDriverManager {
    private Logger LOG = LoggerFactory.getLogger(RdmaDriverManager.class);
    public RdmaShuffleConf conf;
    private final boolean isDriver;
    private RdmaShuffleManagerId localRdmaShuffleManagerId;
    private RdmaNode rdmaNode;
    // Used by diver only
    private final Map<RdmaShuffleManagerId, RdmaChannel> rdmaShuffleManagersMap = new ConcurrentHashMap<>();// shuffleManagerId -> rdmaChannel
    public final Map<BlockManagerId, RdmaShuffleManagerId> blockManagerIdToRdmaShuffleManagerId = new ConcurrentHashMap<>(); // blockManagerId -> rdmaShuffleManagerId

    public RdmaDriverManager(RdmaShuffleConf conf, boolean isDriver) throws Exception {
        this.conf = conf;
        this.isDriver = isDriver;
        if (isDriver) {
            this.rdmaNode = new RdmaNode(conf.getConfKey("morphstream.driver.host"), false, conf, receiveListener);
        }
    }
    public RdmaChannel getRdmaChannelOfREAD_REQUESTOR(RdmaShuffleManagerId rdmaShuffleManagerId, boolean mustRetry) throws IOException, InterruptedException {
        return getRdmaChannel(rdmaShuffleManagerId.getBlockManagerId().getHost(), rdmaShuffleManagerId.getBlockManagerId().getPort(), mustRetry, RdmaChannel.RdmaChannelType.RDMA_READ_REQUESTOR);
    }

    IntFunction<RdmaByteBufferManagedBuffer> getRdmaByteBufferManagedBuffer = length -> {
        try {
            return new RdmaByteBufferManagedBuffer(new RdmaRegisteredBuffer(this.getRdmaBufferManager(),length),length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };
    private RdmaChannel getRdmaChannel(String host, int port, boolean mustRetry, RdmaChannel.RdmaChannelType rdmaChannelType) throws IOException, InterruptedException {
        return this.rdmaNode.getRdmaChannel(new InetSocketAddress(host, port), mustRetry, rdmaChannelType);
    }
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
                    RdmaShuffleManagerHelloRpcMsg helloMsg = (RdmaShuffleManagerHelloRpcMsg) rpcMsg;
                    if (!rdmaShuffleManagersMap.containsKey(helloMsg.rdmaShuffleManagerId)) {
                        //Keep mapping from BlockManagerId to RdmaShuffleManagerId
                        blockManagerIdToRdmaShuffleManagerId.put(helloMsg.rdmaShuffleManagerId.getBlockManagerId(), helloMsg.rdmaShuffleManagerId);
                        // Since we're reusing executor <-> driver QP - which will be taken from cache.
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



    /**
     * Information needed to do RDMA read of remote buffer
     */
    class BufferInfo {
        long address;
        int length;
        int rkey;
        BufferInfo(long address, int length, int rkey) {
            this.address = address;
            this.length = length;
            this.rkey = rkey;
        }
    }

}
