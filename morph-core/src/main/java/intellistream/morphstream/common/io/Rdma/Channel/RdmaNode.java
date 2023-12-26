package intellistream.morphstream.common.io.Rdma.Channel;

import com.ibm.disni.verbs.*;
import intellistream.morphstream.common.io.Rdma.Conf.RdmaChannelConf;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaCompletionListener;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaConnectionListener;
import intellistream.morphstream.common.io.Rdma.Memory.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.RdmaBufferManager;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.ExecutorsServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class RdmaNode {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaNode.class);
    private static final int BACKLOG = 128;
    private final RdmaChannelConf conf;
    private final ConcurrentHashMap<InetSocketAddress, RdmaChannel> activeRdmaChannelMap = new ConcurrentHashMap<>();//保存主动连接其他RDMANode的K,V键值对
    public final ConcurrentHashMap<InetSocketAddress, RdmaChannel> passiveRdmaChannelMap = new ConcurrentHashMap<>();//保存被动接受其他RDMANode的连接的K,V键值对
    public final ConcurrentHashMap<String, InetSocketAddress> passiveRdmaInetSocketMap = new ConcurrentHashMap<>();
    private RdmaBufferManager rdmaBufferManager = null;
    private RdmaCmId listenerRdmaCmId;//Listener Channel ID
    private RdmaEventChannel cmChannel;//RDMA Communication  Event Channel
    private final AtomicBoolean runListenerThread = new AtomicBoolean(false);
    private Thread listeningThread;
    private IbvPd ibvPd;// ibv 保护域 用来注册内存用
    private InetSocketAddress localInetSocketAddress;
    private final ArrayList<Integer> cpuArrayList = new ArrayList<>();
    private int cpuIndex = 0;
    private RdmaChannel.RdmaChannelType rdmaChannelType;
    private String hostName;//本地主机名

    public RdmaNode(String hostName, int port, final RdmaChannelConf conf, RdmaChannel.RdmaChannelType rdmaChannelType) throws Exception {
        this.conf = conf;
        this.rdmaChannelType = rdmaChannelType;
        this.hostName = hostName;

        cmChannel = RdmaEventChannel.createEventChannel();
        if (cmChannel == null) {
            throw new IOException("Failed to create RDMA event channel");
        }
        //create a RdmaCmId
        this.listenerRdmaCmId = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
        if (this.listenerRdmaCmId == null) {
            throw new IOException("Unable to allocate RDMA CM Id");
        }
        try {
            int bindPort = port;
            int err = -1;
            for (int i = 0; i < conf.portMaxRetries(); i++) {
                if (err == 0) {
                    break;
                }
                try {
                    listenerRdmaCmId.bindAddr(new InetSocketAddress(InetAddress.getByName(hostName), bindPort));
                    err = 0;
                } catch (Exception e) {
                    LOG.warn("Failed to bind to port {} on iteration {}, with exception: {}", bindPort, i, e.getMessage());
                    bindPort = bindPort != 0 ? bindPort + 1 : 0;
                }
            }
            if (listenerRdmaCmId.getVerbs() == null) {
                throw new IOException("Failed to bind. Make sure your NIC supports RDMA");
            }
            listenerRdmaCmId.listen(BACKLOG);
            localInetSocketAddress = (InetSocketAddress) listenerRdmaCmId.getSource();
            ibvPd = listenerRdmaCmId.getVerbs().allocPd();
            if (ibvPd == null) {
                throw new IOException("Failed to create PD");
            }
            this.rdmaBufferManager = new RdmaBufferManager(ibvPd, conf);
        } catch (IOException e) {
            LOG.error("Failed in RdmaNode constructor");
            stop();
            throw e;
        } catch (UnsatisfiedLinkError ule) {
            LOG.error("libdisni not found! It must be installed within the java.library.path on each" +
                    " Executor and Driver instance");
            throw ule;
        }
        initCpuArrayList();
    }
    private void initCpuArrayList() throws IOException {
        LOG.info("cpuList from configuration file: {}", conf.cpuList());

        java.util.function.Consumer<Integer> addCpuToList = (cpu) -> {
            // Add CPUs to the list while shuffling the order of the list,
            // so that multiple RdmaNodes on this machine will have a better change
            // to getRdmaBlockLocation different CPUs assigned to them
            cpuArrayList.add(
                    cpuArrayList.isEmpty() ? 0 : new Random().nextInt(cpuArrayList.size()),
                    cpu);
        };

        final int maxCpu = Runtime.getRuntime().availableProcessors() - 1;
        final int maxUsableCpu = Math.min(Runtime.getRuntime().availableProcessors(),
                listenerRdmaCmId.getVerbs().getNumCompVectors()) - 1;
        if (maxUsableCpu < maxCpu - 1) {
            LOG.warn("IbvContext supports only " + (maxUsableCpu + 1) + " CPU cores, while there are" +
                    " " + (maxCpu + 1) + " CPU cores in the system. This may lead to under-utilization of the" +
                    " system's CPU cores. This limitation may be adjustable in the RDMA device configuration.");
        }

        for (String cpuRange : conf.cpuList().split(",")) {
            final String[] cpuRangeArray = cpuRange.split("-");
            int cpuStart, cpuEnd;

            try {
                cpuStart = cpuEnd = Integer.parseInt(cpuRangeArray[0].trim());
                if (cpuRangeArray.length > 1) {
                    cpuEnd = Integer.parseInt(cpuRangeArray[1].trim());
                }

                if (cpuStart > cpuEnd || cpuEnd > maxCpu) {
                    LOG.warn("Invalid cpuList!  start: {}, end: {}, max: {}", cpuStart, cpuEnd, maxCpu);
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                LOG.info("Empty or failure parsing the cpuList. Defaulting to all available CPUs");
                cpuArrayList.clear();
                break;
            }

            for (int cpu = cpuStart; cpu <= Math.min(maxUsableCpu, cpuEnd); cpu++) {
                addCpuToList.accept(cpu);
            }
        }

        if (cpuArrayList.isEmpty()) {
            for (int cpu = 0; cpu <= maxUsableCpu; cpu++) {
                addCpuToList.accept(cpu);
            }
        }

        LOG.info("Using cpuList: {}", cpuArrayList);
    }
    private int getNextCpuVector() {
        return cpuArrayList.get(cpuIndex++ % cpuArrayList.size());
    }
    public RdmaBufferManager getRdmaBufferManager() { return rdmaBufferManager; }
    public void bindConnectCompleteListener(final RdmaConnectionListener connectListener) {
        listeningThread = new Thread(() -> {
            LOG.info("Starting RdmaNode Listening Server, listening on: " + localInetSocketAddress);
            final int teardownListenTimeout = conf.teardownListenTimeout();
            while (runListenerThread.get()) {
                try {
                    RdmaCmEvent event = cmChannel.getCmEvent(teardownListenTimeout);
                    if (event == null) {
                        continue;
                    }
                    RdmaCmId cmId = event.getConnIdPriv();
                    int eventType = event.getEvent();
                    event.ackEvent();

                    InetSocketAddress inetSocketAddress = (InetSocketAddress) cmId.getDestination();

                    if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_REQUEST.ordinal()) {
                        RdmaChannel rdmaChannel = passiveRdmaChannelMap.get(inetSocketAddress);
                        if (rdmaChannel != null) {
                            if (rdmaChannel.isError()) {
                                LOG.warn("Received a redundant RDMA connection request from " + inetSocketAddress + ", which already has an older connection in error state." +
                                        " Removing the old connection and creating a new one");
                                passiveRdmaChannelMap.remove(inetSocketAddress);
                                passiveRdmaInetSocketMap.remove(inetSocketAddress.getHostName());
                                rdmaChannel.stop();
                            } else {
                                LOG.warn("Received a redundant RDMA connection request from " +  inetSocketAddress + ", rejecting the request");
                                continue;
                            }
                        }
                        rdmaChannel = new RdmaChannel(rdmaChannelType, conf, rdmaBufferManager, cmId, getNextCpuVector());
                        if (passiveRdmaChannelMap.putIfAbsent(inetSocketAddress, rdmaChannel) != null) {
                            LOG.warn("Race in creating an RDMA Channel for " + inetSocketAddress);
                            rdmaChannel.stop();
                            continue;
                        }
                        try {
                            rdmaChannel.accept();//Accept a connection request
                        } catch (IOException ioe) {
                            LOG.error("Error in accept call on a passive RdmaChannel: " + ioe);
                            passiveRdmaChannelMap.remove(inetSocketAddress);
                            passiveRdmaInetSocketMap.remove(inetSocketAddress.getAddress().getHostAddress());
                            rdmaChannel.stop();
                        }
                    } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal()) {
                        RdmaChannel rdmaChannel = passiveRdmaChannelMap.get(inetSocketAddress);
                        if (rdmaChannel == null) {
                            LOG.warn("Received an RDMA CM Established Event from " + inetSocketAddress + ", which has no local matching connection. Ignoring");
                            continue;
                        }
                        if (rdmaChannel.isError()) {
                            LOG.warn("Received a redundant RDMA connection request from " + inetSocketAddress +
                                    ", with a local connection in error state. Removing the old connection and " +
                                    "aborting");
                            passiveRdmaChannelMap.remove(inetSocketAddress);
                            passiveRdmaInetSocketMap.remove(inetSocketAddress.getAddress().getHostAddress());
                            rdmaChannel.stop();
                        } else {
                            LOG.info(" rdmaChannel finalizeConnection");
                            rdmaChannel.finalizeConnection();
                            connectListener.onSuccess(inetSocketAddress, rdmaChannel);
                        }
                    } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
                        RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
                        passiveRdmaInetSocketMap.remove(inetSocketAddress.getAddress().getHostAddress());
                        if (rdmaChannel == null) {
                            LOG.info("Received an RDMA CM Disconnect Event from " + inetSocketAddress + ", which has no local matching connection. Ignoring");
                            continue;
                        }

                        rdmaChannel.stop();
                    } else {
                        LOG.info("Received an unexpected CM Event {}", eventType);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, "RDMAConnectionListener");
        listeningThread.setDaemon(true);
        listeningThread.start();

        runListenerThread.set(true);
    }
    public RdmaChannel getRdmaChannel(InetSocketAddress remoteAddr, boolean mustRetry, RdmaChannel.RdmaChannelType rdmaChannelType) throws IOException, InterruptedException {
        final long startTime = System.nanoTime();
        final int maxConnectionAttempts = conf.maxConnectAttempts();
        final long connectionTimeout = maxConnectionAttempts * conf.rdmaCmEventTimeout();
        long elapsedTime = 0;
        int connectionAttempts = 0;

        RdmaChannel rdmaChannel;
        do {
            rdmaChannel = activeRdmaChannelMap.get(remoteAddr);
            if (rdmaChannel == null) {
                rdmaChannel = new RdmaChannel(rdmaChannelType, conf, rdmaBufferManager, getNextCpuVector());

                RdmaChannel actualRdmaChannel = activeRdmaChannelMap.putIfAbsent(remoteAddr, rdmaChannel);
                if (actualRdmaChannel != null) {
                    rdmaChannel = actualRdmaChannel;
                } else {
                    try {
                        rdmaChannel.connect(remoteAddr);
                        LOG.info("Established connection to " + remoteAddr + " in " + (System.nanoTime() - startTime) / 1000000 + " ms");
                    } catch (IOException e) {
                        ++ connectionAttempts;
                        activeRdmaChannelMap.remove(remoteAddr, rdmaChannel);
                        rdmaChannel.stop();
                        if (mustRetry) {
                            if (connectionAttempts == maxConnectionAttempts) {
                                LOG.error("Failed to connect to " + remoteAddr + " after " +
                                        maxConnectionAttempts + " attempts, aborting");
                                throw e;
                            } else {
                                LOG.error("Failed to connect to " + remoteAddr + ", attempt " +
                                        connectionAttempts + " of " + maxConnectionAttempts + " with exception: " + e);
                                continue;
                            }
                        } else {
                            LOG.error("Failed to connect to " + remoteAddr + " with exception: " + e);
                        }
                    }
                }
            }

            if (rdmaChannel.isError()) {
                activeRdmaChannelMap.remove(remoteAddr, rdmaChannel);
                rdmaChannel.stop();
                continue;
            }

            if (!rdmaChannel.isConnected()) {
                rdmaChannel.waitForActiveConnection();
                elapsedTime = (System.nanoTime() - startTime) / 1000000;
            }

            if (rdmaChannel.isConnected()) {
                break;
            }
        } while (mustRetry && (connectionTimeout - elapsedTime) > 0);

        if (mustRetry && !rdmaChannel.isConnected()) {
            throw new IOException("Timeout in establishing a connection to " + remoteAddr.toString());
        }

        return rdmaChannel;
    }
    public void sendRegionTokenToRemote(RdmaChannel rdmaChannel, RegionToken regionToken, String hostName) throws Exception {
        RdmaBuffer rdmaSend = rdmaBufferManager.get(1024);
        ByteBuffer sendBuffer = rdmaSend.getByteBuffer();
        sendBuffer.putInt(regionToken.getSizeInBytes());
        sendBuffer.putLong(regionToken.getAddress());
        sendBuffer.putInt(regionToken.getLocalKey());
        sendBuffer.putInt(regionToken.getRemoteKey());
        CountDownLatch countDownLatch = new CountDownLatch(1);

        rdmaChannel.rdmaSendInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf, Integer IMM) {
                LOG.info("Successfully send regionToken to: " + hostName);
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable exception) {

            }
        }, new long[]{rdmaSend.getAddress()}, new int[]{rdmaSend.getLength()}, new int[]{rdmaSend.getLkey()});

        countDownLatch.await();
    }

    public RegionToken getRemoteRegionToken(RdmaChannel rdmaChannel) throws Exception {
        RdmaBuffer rdmaBuffer = rdmaBufferManager.get(1024);
        ByteBuffer byteBuffer = rdmaBuffer.getByteBuffer();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        rdmaChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf, Integer IMM) {
                LOG.info("Successfully receive request!");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable exception) {

            }
        }, rdmaBuffer.getAddress(), rdmaBuffer.getLength(),rdmaBuffer.getLkey());

        countDownLatch.await();
        int sizeInBytes = byteBuffer.getInt();
        long address = byteBuffer.getLong();
        int localKey = byteBuffer.getInt();
        int remoteKey = byteBuffer.getInt();
        return new RegionToken(sizeInBytes,address,localKey,remoteKey);
    }
    public void stop() throws Exception {
        // Spawn simultaneous disconnect tasks to speed up tear-down
        LinkedList<FutureTask<Void>> futureTaskList = new LinkedList<>();
        //logger.info("activeRdmaChannelMap"+activeRdmaChannelMap);
        //logger.info("passiveRdmaChannelMap"+passiveRdmaChannelMap);
        for (InetSocketAddress inetSocketAddress: activeRdmaChannelMap.keySet()) {
            final RdmaChannel rdmaChannel = activeRdmaChannelMap.remove(inetSocketAddress);
            futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
        }

        // Wait for all of the channels to disconnect
        for (FutureTask<Void> futureTask: futureTaskList) { futureTask.get(); }

        if (runListenerThread.getAndSet(false)) { listeningThread.join(); }

        // Spawn simultaneous disconnect tasks to speed up tear-down
        futureTaskList = new LinkedList<>();
        for (InetSocketAddress inetSocketAddress: passiveRdmaChannelMap.keySet()) {
            final RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
            futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
        }

        for (String hostname : passiveRdmaInetSocketMap.keySet()) {
            passiveRdmaInetSocketMap.remove(hostname);
        }

        // Wait for all of the channels to disconnect
        for (FutureTask<Void> futureTask: futureTaskList) { futureTask.get(); }

        if (rdmaBufferManager != null) { rdmaBufferManager.stop(); }
        if (ibvPd != null) { ibvPd.deallocPd(); }
        if (listenerRdmaCmId != null) { listenerRdmaCmId.destroyId(); }
        if (cmChannel != null) { cmChannel.destroyEventChannel(); }
    }
    private FutureTask<Void> createFutureChannelStopTask(final RdmaChannel rdmaChannel) {
        FutureTask<Void> futureTask = new FutureTask<>(() -> {
            try {
                rdmaChannel.stop();
            } catch (InterruptedException | IOException e) {
                LOG.warn("Exception caught while stopping an RdmaChannel", e);
            }
        }, null);

        // TODO: Use our own ExecutorService in Java
        ExecutorsServiceContext.getInstance().execute(futureTask);
        return futureTask;
    }
}
