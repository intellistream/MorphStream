package intellistream.morphstream.common.io.Rdma;

import com.esotericsoftware.minlog.Log;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RdmaNode {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaNode.class);
    private static final int BACKLOG = 128;
    private final RdmaShuffleConf conf;
    private final ConcurrentHashMap<InetSocketAddress, RdmaChannel> activeRdmaChannelMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetSocketAddress, RdmaChannel> passiveRdmaChannelMap = new ConcurrentHashMap<>();
    private RdmaBufferManager rdmaBufferManager = null;
    private RdmaCmId listenerRdmaCmId;// Context information for RDMA connections
    private RdmaEventChannel cmChannel;// RDMA Event Channel for cmEvents (Connection Manager Events)
    private final AtomicBoolean runThread = new AtomicBoolean(false);
    private Thread listeningThread;
    private IbvPd ibvPd;
    private InetSocketAddress localInetSocketAddress;
    private InetAddress driverInetAddress;
    private final ArrayList<Integer> cpuArrayList = new ArrayList<>();
    private int cpuIndex = 0;
    private final RdmaCompletionListener receiveListener;
    public RdmaNode(String hostName, boolean isWorker, final RdmaShuffleConf conf, final RdmaCompletionListener receiveListener) throws Exception {
        this.conf = conf;
        this.receiveListener = receiveListener;
        try {
            driverInetAddress = InetAddress.getByName(hostName);
            cmChannel = RdmaEventChannel.createEventChannel();//create a communication channel for receiving CM events
            if (this.cmChannel == null) {
                throw new IOException("Unable to allocate RDMA Event Channel");
            }
            //create a RdmaCmId for the server
            this.listenerRdmaCmId = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
            if (this.listenerRdmaCmId == null) {
                throw new IOException("Unable to allocate RDMA CM ID");
            }

            int bindPort = isWorker ? conf.workerPort : conf.driverPort;
            for (int i = 0; i < conf.portMaxRetries; i++) {
                try {
                    listenerRdmaCmId.bindAddr(new InetSocketAddress(InetAddress.getByName(hostName), bindPort));
                } catch (Exception e) {
                    LOG.warn("Failed to bind to port {} on iteration {}", bindPort, i);
                    bindPort = bindPort != 0 ? bindPort + 1 : 0;
                }
            }

            if (listenerRdmaCmId.getVerbs() == null) {
                throw new IOException("Failed to bind. Make sure your NIC supports RDMA");
            }

            initCpuArrayList();
            try {
                listenerRdmaCmId.listen(BACKLOG);
            } catch (IOException e) {
                LOG.error("Failed to start listener: " + e);
            }
            localInetSocketAddress = (InetSocketAddress) listenerRdmaCmId.getSource();

            ibvPd = listenerRdmaCmId.getVerbs().allocPd();
            if (ibvPd == null) {
                throw new IOException("Failed to create PD");
            }
            this.rdmaBufferManager = new RdmaBufferManager(ibvPd, isWorker, conf);
        } catch (IOException e) {
            LOG.error("Exception in RdmaNode constructor: " + e);
            stop();
            throw e;
        } catch (UnsatisfiedLinkError ule) {
            LOG.error("libdisni not found! It must be installed within the java.library.path on each" + " Executor and Driver instance");
            throw ule;
        }

        listeningThread = new Thread(() -> {
            Log.info("Starting RdmaNode Listening Server, listening on: " + localInetSocketAddress);

            final int teardownListenTimeout = conf.teardownListenTimeout;
            while (runThread.get()) {
                try{
                    // Wait for next event
                    RdmaCmEvent event = cmChannel.getCmEvent(teardownListenTimeout);
                    if (event == null) {
                        continue;
                    }

                    RdmaCmId cmId = event.getConnIdPriv();
                    int eventType = event.getEvent();
                    event.ackEvent();
                    InetSocketAddress inetSocketAddress = (InetSocketAddress)cmId.getDestination();

                    if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_REQUEST.ordinal()) {
                        RdmaChannel rdmaChannel = passiveRdmaChannelMap.get(inetSocketAddress);
                        if (rdmaChannel != null) {
                            if (rdmaChannel.isError()) {
                                LOG.warn("Received a redundant RDMA connection request from " + inetSocketAddress + ", which already has an older connection in error state." +
                                        " Removing the old connection and accepting the new one.");
                                passiveRdmaChannelMap.remove(inetSocketAddress);
                                rdmaChannel.stop();
                            } else {
                                LOG.warn("Received a redundant RDMA connection request from " +
                                        inetSocketAddress + ", rejecting the request");
                                continue;
                            }
                        }

                        RdmaChannel.RdmaChannelType rdmaChannelType;
                        if (!isWorker) {
                            rdmaChannelType = RdmaChannel.RdmaChannelType.RPC;
                        } else {
                            rdmaChannelType = RdmaChannel.RdmaChannelType.RDMA_READ_RESPONDER;
                        }

                        rdmaChannel = new RdmaChannel(rdmaChannelType, conf, rdmaBufferManager, receiveListener, cmId, getNextCpuVector());
                        if (passiveRdmaChannelMap.putIfAbsent(inetSocketAddress, rdmaChannel) != null) {
                            LOG.warn("Race in creating an RDMA Channel for " + inetSocketAddress);
                            rdmaChannel.stop();
                            continue;
                        }

                        if (!isWorker) {
                            RdmaChannel previous = activeRdmaChannelMap.put(inetSocketAddress, rdmaChannel);
                            if (previous != null) {
                                previous.stop();
                            }
                        }

                        try {
                            rdmaChannel.accept();
                        } catch (IOException e) {
                            LOG.error("Error in accept call on a passive RdmaChannel: " + e);
                            passiveRdmaChannelMap.remove(inetSocketAddress);
                            rdmaChannel.stop();
                        }
                    } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
                        RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
                        if (rdmaChannel == null) {
                            LOG.info("Received an RDMA CM Disconnect Event from " + inetSocketAddress +
                                    ", which has no local matching connection. Ignoring");
                            continue;
                        }
                        rdmaChannel.stop();
                    } else {
                        LOG.info("Received an unexpected CM Event {}",
                                RdmaCmEvent.EventType.values()[eventType]);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("Exception in RdmaNode listening thread " + e);
                }
            }
            LOG.info("Exiting RdmaNode Listening Server");}, "RdmaNode connection listening thread");
    }
    private void initCpuArrayList() throws IOException {
        LOG.info("cpuList from configuration file: {}", conf.cpuList);
        java.util.function.Consumer<Integer> addCpuToList = (cpu) -> {
            // Add CPUs to the list while shuffling the order of the list,
            // so that multiple RdmaNodes on this machine will have a better change to getRdmaBlockLocation different CPUs assigned to them
            cpuArrayList.add(cpuArrayList.isEmpty() ? 0 : new Random().nextInt(cpuArrayList.size()));
        };

        final int maxCpu = Runtime.getRuntime().availableProcessors() - 1;
        final int maxUsableCpu = Math.min(Runtime.getRuntime().availableProcessors(),listenerRdmaCmId.getVerbs().getNumCompVectors()) -1;
        if (maxUsableCpu < maxCpu -1) {
            LOG.warn("IbvContext supports only " + (maxUsableCpu + 1) + "CPU cores, while there are" + " " + (maxCpu + 1) + " CPU cores in the system. This may lead ti under-utilization of the "+
                    "system's CPU cores. This limitation may be adjustable in the RDMA device configuration.");
        }
        for (String cpuRange : conf.cpuList.split(",")) {
            final String[] cpuRangeArray = cpuRange.split("-");
            int cpuStart, cpuEnd;
            try {
                cpuStart = cpuEnd = Integer.parseInt(cpuRangeArray[0].trim());
                if (cpuRangeArray.length > 1) {
                    cpuEnd = Integer.parseInt(cpuRangeArray[1].trim());
                }

                if (cpuStart > cpuEnd || cpuEnd > maxCpu) {
                    LOG.warn("Invalid cpuList!  start: {}, end: {}, maxCpu: {}", cpuStart, cpuEnd, maxCpu);
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
    public RdmaBufferManager getRdmaBufferManager() { return rdmaBufferManager; }
    public RdmaChannel getRdmaChannel (InetSocketAddress remoteAddr, boolean mustRetry, RdmaChannel.RdmaChannelType rdmaChannelType) throws IOException, InterruptedException {
        final long startTime = System.nanoTime();
        final int maxConnectionAttempts = conf.maxConnectionAttempts;
        final long connectionTimeout = maxConnectionAttempts * conf.rdmaCmEventTimeout;
        long elapsedTime = 0;
        int connectionAttempts = 0;

        RdmaChannel rdmaChannel;
        do {
            rdmaChannel = activeRdmaChannelMap.get(remoteAddr);
            if (rdmaChannel != null) {
                RdmaCompletionListener listener = null;
                if (rdmaChannelType == RdmaChannel.RdmaChannelType.RPC) {
                    // Executor <-> Driver rdma channels need listener on both sides.
                    listener = receiveListener;
                }
                rdmaChannel = new RdmaChannel(rdmaChannelType, conf, rdmaBufferManager, listener, getNextCpuVector());
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
                              LOG.error("Failed to connect to " + remoteAddr + " after " + connectionAttempts + " attempts, aborting");
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
    void stop() throws Exception {
        // Spawn simultaneous disconnect tasks to speed up tear-down
        LinkedList<FutureTask<Void>> futureTaskList = new LinkedList<>();
        for (InetSocketAddress inetSocketAddress: activeRdmaChannelMap.keySet()) {
            final RdmaChannel rdmaChannel = activeRdmaChannelMap.remove(inetSocketAddress);
            futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
        }

        // Wait for all of the channels to disconnect
        for (FutureTask<Void> futureTask: futureTaskList) {
            try {
                futureTask.get(conf.teardownListenTimeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.error("Failed to stop RdmaChannel during " + conf.teardownListenTimeout + " ms");
            } catch (Exception ex) {
                LOG.error(ex.toString());
            }
        }

        if (runThread.getAndSet(false)) { listeningThread.join(conf.teardownListenTimeout); }

        // Spawn simultaneous disconnect tasks to speed up tear-down
        futureTaskList = new LinkedList<>();
        for (InetSocketAddress inetSocketAddress: passiveRdmaChannelMap.keySet()) {
            final RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
            futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
        }

        // Wait for all of the channels to disconnect
        for (FutureTask<Void> futureTask: futureTaskList) {
            try {
                futureTask.get(conf.teardownListenTimeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.error("Failed to stop RdmaChannel during " + conf.teardownListenTimeout + " ms");
            } catch (Exception ex) {
                LOG.error(ex.toString());
            }
        }

        if (rdmaBufferManager != null) { rdmaBufferManager.stop(); }
        if (ibvPd != null) { ibvPd.deallocPd(); }
        if (listenerRdmaCmId != null) { listenerRdmaCmId.destroyId(); }
        if (cmChannel != null) { cmChannel.destroyEventChannel(); }
    }
    private FutureTask<Void> createFutureChannelStopTask(final RdmaChannel rdmaChannel) {
        FutureTask<Void> futureTask = new FutureTask<>(() -> {
            try {
                rdmaChannel.stop();
            } catch (NullPointerException e) {
                LOG.trace("{} already stopped", rdmaChannel);
            } catch (InterruptedException | IOException e) {
                LOG.warn("Exception caught while stopping an RdmaChannel", e);
            }
        }, null);

        // TODO: Use our own ExecutorService in Java
        ExecutionContext.Implicits$.MODULE$.global().execute(futureTask);
        return futureTask;
    }
    private int getNextCpuVector() {
        return cpuArrayList.get(cpuIndex++ % cpuArrayList.size());
    }
    public InetSocketAddress getLocalInetSocketAddress() { return localInetSocketAddress; }
}
