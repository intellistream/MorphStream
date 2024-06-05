package intellistream.morphstream.common.io.Rdma.Channel;
import com.ibm.disni.verbs.*;
import intellistream.morphstream.common.io.Rdma.Conf.RdmaChannelConf;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaCompletionListener;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.Manager.RdmaBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RdmaChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaChannel.class);
    private static final int MAX_ACK_COUNT = 4;
    private static final int POLL_CQ_LIST_SIZE = 16;
    private static final int ZERO_SIZED_RECV_WR_LIST_SIZE = 16;
    private static final AtomicInteger idGenerator = new AtomicInteger(0);
    private final int id = idGenerator.getAndIncrement();
    private RdmaCmId cmId = null;//RDMA Connection Management Identifier (Key information and status of the connection)
    private final ConcurrentHashMap<Integer, ConcurrentLinkedDeque<SVCPostSend>> svcPostSendCache = new ConcurrentHashMap();
    private final ConcurrentHashMap<Integer, ConcurrentLinkedDeque<SVCPostRecv>> svcPostRecvCache = new ConcurrentHashMap();
    private final ConcurrentLinkedDeque<PendingSend> sendWrQueue = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<PendingReceive> recvWrQueue = new ConcurrentLinkedDeque<>();
    private final RdmaChannelType rdmaChannelType;
    private final RdmaBufferManager rdmaBufferManager;
    private RdmaEventChannel eventChannel = null;
    private IbvCompChannel compChannel = null;//The channel to async notify application, that event completion has occurred
    private IbvCQ cq = null;//Queue to store completion events
    private IbvQP qp = null;//Queue Pair
    //Timeouts settings
    private final int rdmaCmEventTimeout;
    private final int teardownListenTimeout;
    private final int resolvePathTimeout;

    private static final int RECV_CREDIT_REPORT_RATIO = 8; // Send a credit report on every (recvDepth / RECV_CREDIT_REPORT_RATIO) receive credit reclaims
    private Semaphore remoteRecvCredits;
    private int localRecvCreditsPendingReport = 0;

    private Semaphore sendBudgetSemaphore;
    private Semaphore recvBudgetSemaphore;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    public enum RdmaChannelType { RPC, RDMA_READ_REQUESTOR, RDMA_READ_RESPONDER, RDMA_WRITE_REQUESTOR, RDMA_WRITE_RESPONDER }
    private PostRecvWr[] postRecvWrArray = null;

    private int ackCounter = 0;
    private final int sendDepth;
    private final int recvDepth;
    private boolean isWarnedOnSendOverSubscription = false;
    private final int cpuVector;
    private SVCReqNotify reqNotifyCall;
    private SVCPollCq svcPollCq;
    private IbvWC[] ibvWCs;

    private final ConcurrentHashMap<Integer, CompletionInfo> completionInfoMap =  new ConcurrentHashMap<>();
    private static final int NOOP_RESERVED_INDEX = 0;// NOOP_RESERVED_INDEX is used for send operations that do not require a callback
    private final AtomicInteger completionInfoIndex = new AtomicInteger(NOOP_RESERVED_INDEX);

    private RdmaThread rdmaThread = null;

    enum RdmaChannelState { IDLE, CONNECTING, CONNECTED, ERROR }
    private final AtomicInteger rdmaChannelState = new AtomicInteger(RdmaChannelState.IDLE.ordinal());
    private void setRdmaChannelState(RdmaChannelState newRdmaChannelState) {
        // Allow to change the channel state only if not in ERROR
        rdmaChannelState.updateAndGet(state -> state != RdmaChannelState.ERROR.ordinal() ? newRdmaChannelState.ordinal() : state);
    }
    private final RdmaChannelConf conf;
    RdmaChannel(
            RdmaChannelType rdmaChannelType,
            RdmaChannelConf conf,
            RdmaBufferManager rdmaBufferManager,
            RdmaCmId cmId,
            int cpuVector) {
        this(rdmaChannelType, conf, rdmaBufferManager, cpuVector);
        this.cmId = cmId;
    }
    RdmaChannel(RdmaChannelType rdmaChannelType, RdmaChannelConf conf, RdmaBufferManager rdmaBufferManager, int cpuVector){
        this.rdmaChannelType = rdmaChannelType;
        this.rdmaBufferManager = rdmaBufferManager;
        this.cpuVector = cpuVector;
        this.conf = conf;
        switch (rdmaChannelType) {
            case RPC:
                //Single bidirectional QP between executors and driver.
                if (conf.swFlowControl()) {
                    this.remoteRecvCredits = new Semaphore(conf.recvQueueDepth() - RECV_CREDIT_REPORT_RATIO, false);
                }
                this.recvDepth = conf.recvQueueDepth();
                this.sendDepth = conf.sendQueueDepth();
                this.sendBudgetSemaphore = new Semaphore(sendDepth - RECV_CREDIT_REPORT_RATIO, false);
                this.recvBudgetSemaphore = new Semaphore(recvDepth,false);
                break;

            case RDMA_READ_REQUESTOR:

            case RDMA_WRITE_REQUESTOR:

            case RDMA_WRITE_RESPONDER:

            case RDMA_READ_RESPONDER:
                // Requires sends and receives
                this.recvDepth = conf.recvQueueDepth();
                this.sendDepth = conf.sendQueueDepth();
                this.sendBudgetSemaphore = new Semaphore(sendDepth - RECV_CREDIT_REPORT_RATIO, false);
                this.recvBudgetSemaphore = new Semaphore(recvDepth,false);
                break;

            default:
                throw new IllegalArgumentException("Illegal RdmaChannelType");
        }
        this.rdmaCmEventTimeout = conf.rdmaCmEventTimeout();
        this.teardownListenTimeout = conf.teardownListenTimeout();
        this.resolvePathTimeout = conf.resolvePathTimeout();
    }

    private int putCompletionInfo(CompletionInfo completionInfo) {
        int index;
        do {
            index = completionInfoIndex.incrementAndGet();
        } while (index == NOOP_RESERVED_INDEX);
        CompletionInfo retCompletionInfo = completionInfoMap.put(index, completionInfo);
        if (retCompletionInfo != null) {
            throw new IllegalStateException("CompletionInfoMap already contains index " + index);
        }
        return index;
    }
    private CompletionInfo removeCompletionInfo(int index) {
        return completionInfoMap.remove(index);
    }
    private void setupCommon() throws IOException {
        IbvContext ibvContext = cmId.getVerbs();
        if (ibvContext == null) {
            throw new IOException("Failed to retrieve IbvContext");
        }

        compChannel = ibvContext.createCompChannel();
        if (compChannel == null) {
            throw new IOException("createCompChannel() failed");
        }

        // ncqe must be greater than 1
        cq = ibvContext.createCQ(compChannel, (sendDepth + recvDepth) > 0 ? sendDepth + recvDepth : 1, cpuVector);
        if (cq == null) {
            throw new IOException("createCQ() failed");
        }

        reqNotifyCall = cq.reqNotification(false);
        reqNotifyCall.execute();

        ibvWCs = new IbvWC[POLL_CQ_LIST_SIZE];
        for (int i = 0; i < POLL_CQ_LIST_SIZE; i++) {
            ibvWCs[i] = new IbvWC();
        }
        svcPollCq = cq.poll(ibvWCs, POLL_CQ_LIST_SIZE);

        IbvQPInitAttr attr = new IbvQPInitAttr();
        attr.setQp_type(IbvQP.IBV_QPT_RC);
        attr.setSend_cq(cq);
        attr.setRecv_cq(cq);
        attr.cap().setMax_recv_sge(1);
        attr.cap().setMax_recv_wr(recvDepth);
        attr.cap().setMax_send_sge(1);
        attr.cap().setMax_send_wr(sendDepth);

        qp = cmId.createQP(rdmaBufferManager.getPd(), attr);
        if (qp == null) {
            throw new IOException("createQP() failed");
        }

        rdmaThread = new RdmaThread(this, cpuVector);
        rdmaThread.start();
    }

    /**
     * Connect to a remote address
     * @param socketAddress
     * @throws IOException
     */
    public void connect(InetSocketAddress socketAddress) throws IOException {
        eventChannel = RdmaEventChannel.createEventChannel();
        if (eventChannel == null) {
            throw new IOException("createEventChannel() failed");
        }

        // Create an active connect cmId
        cmId = eventChannel.createId(RdmaCm.RDMA_PS_TCP);
        if (cmId == null) {
            throw new IOException("createId() failed");
        }

        // Resolve the addr
        setRdmaChannelState(RdmaChannelState.CONNECTING);
        cmId.resolveAddr(null, socketAddress, resolvePathTimeout);

        processRdmaCmEvent(RdmaCmEvent.EventType.RDMA_CM_EVENT_ADDR_RESOLVED.ordinal(), rdmaCmEventTimeout);

        // Resolve the route
        cmId.resolveRoute(resolvePathTimeout);

        processRdmaCmEvent(RdmaCmEvent.EventType.RDMA_CM_EVENT_ROUTE_RESOLVED.ordinal(), rdmaCmEventTimeout);

        setupCommon();

        RdmaConnParam connParams = new RdmaConnParam();
        connParams.setRetry_count((byte) 7);
        connParams.setRnr_retry_count((byte) 7);
        cmId.connect(connParams);

        processRdmaCmEvent(RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal(), rdmaCmEventTimeout);
        setRdmaChannelState(RdmaChannelState.CONNECTED);
    }
    public void finalizeConnection() {
        setRdmaChannelState(RdmaChannelState.CONNECTED);
        synchronized (rdmaChannelState) { rdmaChannelState.notifyAll(); }
    }
    public void waitForActiveConnection() {
        synchronized (rdmaChannelState) {
            try {
                rdmaChannelState.wait(100);
            } catch (InterruptedException ignored) {}
        }
    }

    /**
     * Accept a connection request
     * @throws IOException
     */
    public void accept() throws IOException {
        RdmaConnParam connParams = new RdmaConnParam();
        setupCommon();
        connParams.setRetry_count((byte) 7);
        connParams.setRnr_retry_count((byte) 7);
        setRdmaChannelState(RdmaChannelState.CONNECTING);
        cmId.accept(connParams);
    }

    private void rdmaPostSendWRList(LinkedList<IbvSendWR> sendWRList) throws IOException {
        if (isError() || isStopped.get()) {
            throw new IOException("QP is in error state, can't post new requests");
        }

        ConcurrentLinkedDeque<SVCPostSend> stack;
        SVCPostSend svcPostSendObject;

        int numWrElements = sendWRList.size();
        // Special case for 0 sgeElements when rdmaSendWithImm
        if (sendWRList.size() == 1 && sendWRList.getFirst().getNum_sge() == 0) {
            numWrElements = NOOP_RESERVED_INDEX;
        }

        stack = svcPostSendCache.computeIfAbsent(numWrElements, numElements -> new ConcurrentLinkedDeque<>());

        // To avoid buffer allocations in disni update cached SVCPostSendObject
        if (sendWRList.getFirst().getOpcode() == IbvSendWR.IbvWrOcode.IBV_WR_RDMA_READ.ordinal()
                && (svcPostSendObject = stack.pollFirst()) != null) {
            int i = 0;
            for (IbvSendWR sendWr: sendWRList) {
                SVCPostSend.SendWRMod sendWrMod = svcPostSendObject.getWrMod(i);

                sendWrMod.setWr_id(sendWr.getWr_id());
                sendWrMod.setSend_flags(sendWr.getSend_flags());
                // Setting up RDMA attributes
                sendWrMod.getRdmaMod().setRemote_addr(sendWr.getRdma().getRemote_addr());
                sendWrMod.getRdmaMod().setRkey(sendWr.getRdma().getRkey());
                sendWrMod.getRdmaMod().setReserved(sendWr.getRdma().getReserved());

                if (sendWr.getNum_sge() == 1) {
                    IbvSge sge = sendWr.getSge(0);
                    sendWrMod.getSgeMod(0).setLkey(sge.getLkey());
                    sendWrMod.getSgeMod(0).setAddr(sge.getAddr());
                    sendWrMod.getSgeMod(0).setLength(sge.getLength());
                }
                i++;
            }
        } else {
            svcPostSendObject = qp.postSend(sendWRList, null);
        }

        svcPostSendObject.execute();
        // Cache SVCPostSend objects only for RDMA Read requests
        if (sendWRList.getFirst().getOpcode() == IbvSendWR.IbvWrOcode.IBV_WR_RDMA_READ.ordinal()) {
            stack.add(svcPostSendObject);
        } else {
            svcPostSendObject.free();
        }
    }
    /**
     * rdmaPostRecvWRList
     * @param recvWRList recvWorkRequest List
     * @throws IOException
     */
    private void rdmaPostRecvWRList(LinkedList<IbvRecvWR> recvWRList) throws IOException {
        if (isError() || isStopped.get()) {
            throw new IOException("QP is in error state, can't post new requests");
        }

        SVCPostRecv svcPostRecvObject;

        svcPostRecvObject = qp.postRecv(recvWRList, null);

        svcPostRecvObject.execute();
        LOG.debug("svcPostRecvObject execute");
        svcPostRecvObject.free();
    }

    private void rdmaPostWRListInQueue(PendingSend pendingSend) throws IOException {
        if (isError() || isStopped.get()) {
            throw new IOException("QP is in error state, can't post new requests");
        }

        if(conf.swOrderControl()) {
            //Work Request Element consumption strict order guarantee and guarantee remoteRecvCredits
            try {
                sendBudgetSemaphore.acquire(pendingSend.ibvSendWRList.size());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                rdmaPostSendWRList(pendingSend.ibvSendWRList);
            } catch (Exception e) {
                sendBudgetSemaphore.release(pendingSend.ibvSendWRList.size());
                throw e;
            }
        } else {
            // work request element don't consumption strict order guarantee
            if (sendBudgetSemaphore.tryAcquire(pendingSend.ibvSendWRList.size())) {
                // Ordering is lost here since if there are credits avail they will be immediately utilized
                // without fairness. We don't care about fairness, since Spark doesn't expect the requests to complete in a particular order
                if (pendingSend.recvCreditsNeeded > 0 && remoteRecvCredits != null && !remoteRecvCredits.tryAcquire(pendingSend.recvCreditsNeeded)) {
                    sendBudgetSemaphore.release(pendingSend.ibvSendWRList.size());
                    sendWrQueue.add(pendingSend);
                } else {
                    try {
                        rdmaPostSendWRList(pendingSend.ibvSendWRList);
                    } catch (Exception e) {
                        if (remoteRecvCredits != null) {
                            remoteRecvCredits.release(pendingSend.recvCreditsNeeded);
                        }
                        sendBudgetSemaphore.release(pendingSend.ibvSendWRList.size());
                        sendWrQueue.add(pendingSend);
                        throw e;
                    }
                }
            } else {
                if (!isWarnedOnSendOverSubscription) {
                    isWarnedOnSendOverSubscription = true;
                    LOG.warn(this + " oversubscription detected. RDMA" + " send queue depth is too small. To improve performance, please set" + "rdma.sendQueueDepth to a higher value (current depth: " + sendDepth);
                }
                sendWrQueue.add(pendingSend);

                // Try again, in case it is the only WR in the queue and there are no pending sends
                if (sendBudgetSemaphore.tryAcquire(pendingSend.ibvSendWRList.size())) {
                    if (sendWrQueue.remove(pendingSend)) {
                        if (pendingSend.recvCreditsNeeded > 0 &&
                                remoteRecvCredits != null &&
                                !remoteRecvCredits.tryAcquire(pendingSend.recvCreditsNeeded)) {
                            sendBudgetSemaphore.release(pendingSend.ibvSendWRList.size());
                            sendWrQueue.add(pendingSend);
                        } else {
                            try {
                                rdmaPostSendWRList(pendingSend.ibvSendWRList);
                            } catch (Exception e) {
                                if (remoteRecvCredits != null) {
                                    remoteRecvCredits.release(pendingSend.recvCreditsNeeded);
                                }
                                sendBudgetSemaphore.release(pendingSend.ibvSendWRList.size());
                                sendWrQueue.add(pendingSend);
                                throw e;
                            }
                        }
                    } else {
                        sendBudgetSemaphore.release(pendingSend.ibvSendWRList.size());
                    }
                }
            }
        }
    }

    private void rdmaPostWRListInQueue(PendingReceive pendingReceive) throws IOException {
        if (isError() || isStopped.get()) {
            throw new IOException("QP is in error state, can't post new requests");
        }

        if(conf.swOrderControl()) {
            // Work Request Element consumption strict order guarantee
            try {
                recvBudgetSemaphore.acquire(pendingReceive.ibvRecvWRList.size());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                rdmaPostRecvWRList(pendingReceive.ibvRecvWRList);
            } catch (Exception e) {
                recvBudgetSemaphore.release(pendingReceive.ibvRecvWRList.size());
                throw e;
            }
        }else {
            // Work Request Element don't consumption strict order guarantee
            if (recvBudgetSemaphore.tryAcquire(pendingReceive.ibvRecvWRList.size())) {
                // Ordering is lost here since if there are credits avail they will be immediately utilized
                // without fairness. We don't care about fairness, since Spark doesn't expect the requests to
                // complete in a particular order
                try {
                    rdmaPostRecvWRList(pendingReceive.ibvRecvWRList);
                } catch (Exception e) {
                    recvBudgetSemaphore.release(pendingReceive.ibvRecvWRList.size());
                    recvWrQueue.add(pendingReceive);
                    throw e;
                }
            } else {
                if (!isWarnedOnSendOverSubscription) {
                    isWarnedOnSendOverSubscription = true;
                    LOG.warn(this + " oversubscription detected. RDMA" +
                            " send queue depth is too small. To improve performance, please set" +
                            " spark.shuffle.rdma.sendQueueDepth to a higher value (current depth: " + sendDepth);
                }
                recvWrQueue.add(pendingReceive);

                // Try again, in case it is the only WR in the queue and there are no pending sends
                if (recvBudgetSemaphore.tryAcquire(pendingReceive.ibvRecvWRList.size())) {
                    if (sendWrQueue.remove(pendingReceive)) {
                        try {
                            rdmaPostRecvWRList(pendingReceive.ibvRecvWRList);
                        } catch (Exception e) {
                            recvBudgetSemaphore.release(pendingReceive.ibvRecvWRList.size());
                            recvWrQueue.add(pendingReceive);
                            throw e;
                        }
                    } else {
                        recvBudgetSemaphore.release(pendingReceive.ibvRecvWRList.size());
                    }
                }
            }
        }
    }

    public void rdmaReadInQueue(RdmaCompletionListener listener, long localAddress, int lKey,
                                int[] sizes, long[] remoteAddresses, int[] rKeys) throws IOException {
        long offset = 0;
        LinkedList<IbvSendWR> readWRList = new LinkedList<>();
        for (int i = 0; i < remoteAddresses.length; i++) {
            IbvSge readSge = new IbvSge();
            readSge.setAddr(localAddress + offset);
            readSge.setLength(sizes[i]);
            readSge.setLkey(lKey);
            offset += sizes[i];

            LinkedList<IbvSge> readSgeList = new LinkedList<>();
            readSgeList.add(readSge);

            IbvSendWR readWr = new IbvSendWR();
            readWr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_RDMA_READ.ordinal());
            readWr.setSg_list(readSgeList);
            readWr.getRdma().setRemote_addr(remoteAddresses[i]);
            readWr.getRdma().setRkey(rKeys[i]);

            readWRList.add(readWr);
        }

        readWRList.getLast().setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        int completionInfoId = putCompletionInfo(new CompletionInfo(listener, remoteAddresses.length));
        readWRList.getLast().setWr_id(completionInfoId);

        try {
            rdmaPostWRListInQueue(new PendingSend(readWRList, 0));
        } catch (Exception e) {
            removeCompletionInfo(completionInfoId);
            throw e;
        }
    }

    /**
     * RDMA write buffer(localAddress, localLength, lKey) to remote buffer at remoteAddress
     * @param listener
     * @param localAddress
     * @param localLength
     * @param lKey
     * @param remoteAddress
     * @param rKey
     * @throws IOException
     */
    public void rdmaWriteInQueue(RdmaCompletionListener listener, long localAddress, int localLength, int lKey, long remoteAddress, int rKey) throws IOException {
        LinkedList<IbvSendWR> writeWRList = new LinkedList<>();

        IbvSge writeSge = new IbvSge();
        writeSge.setAddr(localAddress);
        writeSge.setLength(localLength);
        writeSge.setLkey(lKey);

        LinkedList<IbvSge> writeSgeList = new LinkedList<>();
        writeSgeList.add(writeSge);

        IbvSendWR writeWr = new IbvSendWR();
        writeWr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_RDMA_WRITE.ordinal());
        writeWr.setSg_list(writeSgeList);
        writeWr.getRdma().setRemote_addr(remoteAddress);
        writeWr.getRdma().setRkey(rKey);
        writeWr.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        writeWRList.add(writeWr);

        int completionInfoId = putCompletionInfo(new CompletionInfo(listener, 1));
        writeWRList.getLast().setWr_id(completionInfoId);

        try {
            rdmaPostWRListInQueue(new PendingSend(writeWRList, 0));
        } catch (Exception e) {
            removeCompletionInfo(completionInfoId);
            throw e;
        }
    }
    public void rdmaCASInQueue(RdmaCompletionListener listener, long localAddress, int localLength, int lKey, long remoteAddress, int rKey, long compareValue, long swapValue) throws IOException {
        LinkedList<IbvSendWR> casWRList = new LinkedList<>();

        IbvSge casSge = new IbvSge();
        casSge.setAddr(localAddress);
        casSge.setLength(localLength);
        casSge.setLkey(lKey);

        LinkedList<IbvSge> casSgeList = new LinkedList<>();
        casSgeList.add(casSge);

        IbvSendWR casWr = new IbvSendWR();
        casWr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_ATOMIC_CMP_AND_SWP.ordinal());
        casWr.setSg_list(casSgeList);
        casWr.getRdma().setRkey(rKey);
        casWr.getRdma().setRemote_addr(remoteAddress);
        casWr.getAtomic().setRemote_addr(remoteAddress);
        casWr.getAtomic().setRkey(rKey);
        casWr.getAtomic().setCompare_add(compareValue);
        casWr.getAtomic().setSwap(swapValue);
        casWr.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        casWRList.add(casWr);

        int completionInfoId = putCompletionInfo(new CompletionInfo(listener, 1));
        casWRList.getLast().setWr_id(completionInfoId);

        try {
            rdmaPostWRListInQueue(new PendingSend(casWRList, 0));
        } catch (Exception e) {
            removeCompletionInfo(completionInfoId);
            throw e;
        }
    }
    public void rdmaFAAInQueue(RdmaCompletionListener listener, long localAddress, int localLength, int lKey, long remoteAddress, int rkey, long incrementValue) throws IOException {
        LinkedList<IbvSendWR> faaWRList = new LinkedList<>();

        IbvSge faaSge = new IbvSge();
        faaSge.setAddr(localAddress);
        faaSge.setLength(localLength);
        faaSge.setLkey(lKey);

        LinkedList<IbvSge> faaSgeList = new LinkedList<>();
        faaSgeList.add(faaSge);

        IbvSendWR faaWr = new IbvSendWR();
        faaWr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_ATOMIC_FETCH_AND_ADD.ordinal());
        faaWr.setSg_list(faaSgeList);
        faaWr.getRdma().setRkey(rkey);
        faaWr.getRdma().setRemote_addr(remoteAddress);
        faaWr.getAtomic().setCompare_add(incrementValue);
        faaWr.getAtomic().setRkey(rkey);
        faaWr.getAtomic().setRemote_addr(remoteAddress);
        faaWr.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        faaWRList.add(faaWr);

        int completionInfoId = putCompletionInfo(new CompletionInfo(listener, 1));
        faaWRList.getLast().setWr_id(completionInfoId);

        try {
            rdmaPostWRListInQueue(new PendingSend(faaWRList, 0));
        } catch (Exception e) {
            removeCompletionInfo(completionInfoId);
            throw e;
        }
    }

    public void rdmaSendInQueue(RdmaCompletionListener listener, long[] localAddresses, int[] sizes,  int[] lKeys) throws IOException {
        LinkedList<IbvSendWR> sendWRList = new LinkedList<>();
        for (int i = 0; i < localAddresses.length; i++) {
            IbvSge sendSge = new IbvSge();
            sendSge.setAddr(localAddresses[i]);
            sendSge.setLength(sizes[i]);
            sendSge.setLkey(lKeys[i]);

            LinkedList<IbvSge> sendSgeList = new LinkedList<>();
            sendSgeList.add(sendSge);

            IbvSendWR sendWr = new IbvSendWR();
            sendWr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());
            sendWr.setSg_list(sendSgeList);

            sendWRList.add(sendWr);
        }

        sendWRList.getLast().setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        int completionInfoId = putCompletionInfo(new CompletionInfo(listener, localAddresses.length));
        sendWRList.getLast().setWr_id(completionInfoId);

        try {
            rdmaPostWRListInQueue(new PendingSend(sendWRList, sendWRList.size()));
        } catch (Exception e) {
            removeCompletionInfo(completionInfoId);
            throw e;
        }
    }

    // TODO RDMA ReceiveInQueue
    public void rdmaReceiveInQueue(RdmaCompletionListener listener, long localAddresses,
                                   int length, int lKeys) throws Exception{
        LinkedList<IbvRecvWR> recvWrList = new LinkedList<>();

        IbvSge sge = new IbvSge();
        sge.setAddr(localAddresses);
        sge.setLength(length);
        sge.setLkey(lKeys);

        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sge);

        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeList);
        recvWrList.add(recvWR);

        int completionInfoId = putCompletionInfo(new CompletionInfo(listener, 1));
        recvWR.setWr_id(completionInfoId);

        try {
            rdmaPostWRListInQueue(new PendingReceive(recvWrList, recvWrList.size()));
        } catch (Exception e) {
            removeCompletionInfo(completionInfoId);
            throw e;
        }
    }

    // Used only for sending a receive credit report
    public void rdmaSendWithImm(int immData) throws IOException {
        LinkedList<IbvSendWR> sendWRList = new LinkedList<>();
        LinkedList<IbvSge> sendSgeList = new LinkedList<>();
        IbvSendWR sendWr = new IbvSendWR();
        sendWr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_RDMA_WRITE_WITH_IMM.ordinal());
        sendWr.setImm_data(immData);
        sendWr.setSg_list(sendSgeList);
        sendWr.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWr.setWr_id(NOOP_RESERVED_INDEX); // doesn't require a callback
        sendWRList.add(sendWr);

        rdmaPostSendWRList(sendWRList);
    }

    public void rdmaRecvWithImm(RdmaCompletionListener listener) throws IOException {
        LinkedList<IbvRecvWR> recvWrList = new LinkedList<>();
        IbvRecvWR recvWR = new IbvRecvWR();
        recvWrList.add(recvWR);
        int completionInfoId = putCompletionInfo(new CompletionInfo(listener, 1));
        recvWR.setWr_id(completionInfoId);

        try {
            rdmaPostWRListInQueue(new PendingReceive(recvWrList, recvWrList.size()));
        } catch (Exception e) {
            removeCompletionInfo(completionInfoId);
            throw e;
        }
    }

    private void exhaustCq() throws IOException {
        int reclaimedSendPermits = 0;
        int reclaimedRecvWrs = 0;
        int firstRecvWrIndex = -1;

        while (true) {
            int res = svcPollCq.execute().getPolls();
            if (res < 0) {
                LOG.error("PollCQ failed executing with res: " + res);
                break;
            } else if (res > 0) {
                for (int i = 0; i < res; i++) {
                    boolean wcSuccess = ibvWCs[i].getStatus() == IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal();
                    if (!wcSuccess && !isError()) {
                        LOG.error(String.valueOf(ibvWCs[i].getOpcode()));
                        setRdmaChannelState(RdmaChannelState.ERROR);
                        LOG.error("Completion with error: " + IbvWC.IbvWcStatus.values()[ibvWCs[i].getStatus()].name());
                    }

                    if (ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.getOpcode() ||
                            ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RDMA_WRITE.getOpcode() ||
                            ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RDMA_READ.getOpcode() ||
                            ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_COMP_SWAP.getOpcode() ||
                            ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_FETCH_ADD.getOpcode()){
                        int completionInfoId = (int) ibvWCs[i].getWr_id();
                        if (completionInfoId != NOOP_RESERVED_INDEX) {
                            CompletionInfo completionInfo = removeCompletionInfo(completionInfoId);
                            if (completionInfo != null) {
                                if (wcSuccess) {
                                    completionInfo.listener.onSuccess(null, null);
                                } else {
                                    completionInfo.listener.onFailure(
                                            new IOException("RDMA Send/Write/Read WR completed with error: " +
                                                    IbvWC.IbvWcStatus.values()[ibvWCs[i].getStatus()].name()));
                                }

                                reclaimedSendPermits += completionInfo.sendPermitsToReclaim;
                            } else if (wcSuccess) {
                                // Ignore the case of error, as the listener will be invoked by the last WC
                                LOG.warn("Couldn't find CompletionInfo with index: " + completionInfoId);
                            }
                        }
                    } else if (ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()) {
                        int recvWrId = (int)ibvWCs[i].getWr_id();
                        if (firstRecvWrIndex == -1) {
                            firstRecvWrIndex = recvWrId;
                        }
                        if (recvWrId != NOOP_RESERVED_INDEX) {
                            CompletionInfo completionInfo = removeCompletionInfo(recvWrId);
                            if (completionInfo != null) {
                                if (wcSuccess) {
                                    completionInfo.listener.onSuccess(null, null);
                                } else {
                                    completionInfo.listener.onFailure(
                                            new IOException(this + "RDMA Receive WR completed with error: " + IbvWC.IbvWcStatus.values()[ibvWCs[i].getStatus()]));
                                }

                                reclaimedRecvWrs += 1;
                            } else if (wcSuccess) {
                                // Ignore the case of error, as the listener will be invoked by the last WC
                                LOG.warn("Couldn't find CompletionInfo with index: " + reclaimedRecvWrs);
                            }
                        }
                    } else if (ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV_RDMA_WITH_IMM.getOpcode()) {
                        //TODO Software-level flow control enabled can't use sendIMM
                        if (conf.swFlowControl()){
                            // Receive credit report - update new credits
                            if (remoteRecvCredits != null) {
                                remoteRecvCredits.release(ibvWCs[i].getImm_data());
                            }
                            int recvWrId = (int)ibvWCs[i].getWr_id();
                            if (firstRecvWrIndex == -1) {
                                firstRecvWrIndex = recvWrId;
                            }
                            reclaimedRecvWrs += 1;
                        } else {
                            //Software-level flow control is disabled
                            int recvWrId = (int)ibvWCs[i].getWr_id();
                            if (recvWrId != NOOP_RESERVED_INDEX) {
                                CompletionInfo completionInfo = removeCompletionInfo(recvWrId);
                                if (completionInfo != null) {
                                    if (wcSuccess) {
                                        completionInfo.listener.onSuccess(null, ibvWCs[i].getImm_data());
                                    } else {
                                        completionInfo.listener.onFailure(
                                                new IOException("RDMA SendIMM WR completed with error: " +
                                                        IbvWC.IbvWcStatus.values()[ibvWCs[i].getStatus()].name()));
                                    }

                                    reclaimedSendPermits += completionInfo.sendPermitsToReclaim;
                                } else if (wcSuccess) {
                                    // Ignore the case of error, as the listener will be invoked by the last WC
                                    LOG.warn("Couldn't find CompletionInfo with index: " + recvWrId);
                                }
                            }
                        }
                    } else {
                        LOG.error(this + "Unexpected opcode in PollCQ: " + ibvWCs[i].getOpcode());
                    }
                }
            } else {
                break;
            }
        }

        if (isError()) {
            throw new IOException(this + "QP entered ERROR state");
        }

        if (conf.swFlowControl() && rdmaChannelType == RdmaChannelType.RPC) {
            // Software-level flow control is enabled
            localRecvCreditsPendingReport += reclaimedRecvWrs;
            if (localRecvCreditsPendingReport > (recvDepth / RECV_CREDIT_REPORT_RATIO)) {
                // Send a credit report once (recvDepth / RECV_CREDIT_REPORT_RATIO) were accumulated
                try {
                    rdmaSendWithImm(localRecvCreditsPendingReport);
                } catch (IOException ioe) {
                    LOG.warn(this + " Failed to send a receive credit report with exception: " + ioe +
                            " failing silently.");
                }
                localRecvCreditsPendingReport = 0;
            }
        }

        if(conf.swOrderControl()) {
            // Work Request Element consumption strict order guarantee
            sendBudgetSemaphore.release(reclaimedSendPermits);
            recvBudgetSemaphore.release(reclaimedRecvWrs);
        }else {
            // Work Request Element don't consumption strict order guarantee
            // Drain pending sends queue
            while (sendBudgetSemaphore != null && !isStopped.get() && !isError()) {
                PendingSend pendingSend = sendWrQueue.poll();
                if (pendingSend != null) {
                    // If there are not enough available permits from
                    // this run AND from the semaphore, then it means that there are
                    // more completions coming and they will exhaust the queue later
                    if (pendingSend.ibvSendWRList.size() > reclaimedSendPermits) {
                        if (!sendBudgetSemaphore.tryAcquire(
                                pendingSend.ibvSendWRList.size() - reclaimedSendPermits)) {
                            sendWrQueue.push(pendingSend);
                            sendBudgetSemaphore.release(reclaimedSendPermits);
                            break;
                        } else {
                            if (pendingSend.recvCreditsNeeded > 0 &&
                                    remoteRecvCredits != null &&
                                    !remoteRecvCredits.tryAcquire(pendingSend.recvCreditsNeeded)) {
                                sendWrQueue.push(pendingSend);
                                sendBudgetSemaphore.release(pendingSend.ibvSendWRList.size());
                                break;
                            } else {
                                reclaimedSendPermits = 0;
                            }
                        }
                    } else {
                        if (pendingSend.recvCreditsNeeded > 0 &&
                                remoteRecvCredits != null &&
                                !remoteRecvCredits.tryAcquire(pendingSend.recvCreditsNeeded)) {
                            sendWrQueue.push(pendingSend);
                            sendBudgetSemaphore.release(reclaimedSendPermits);
                            break;
                        } else {
                            reclaimedSendPermits -= pendingSend.ibvSendWRList.size();
                        }
                    }

                    try {
                        rdmaPostSendWRList(pendingSend.ibvSendWRList);
                    } catch (IOException e) {
                        setRdmaChannelState(RdmaChannelState.ERROR);
                        // reclaim the credit and put sendWRList back to the queue
                        // however, the channel/QP is already broken and more actions
                        // needed to be taken to recover
                        reclaimedSendPermits += pendingSend.ibvSendWRList.size();
                        if (remoteRecvCredits != null) {
                            remoteRecvCredits.release(pendingSend.recvCreditsNeeded);
                        }
                        sendWrQueue.push(pendingSend);
                        sendBudgetSemaphore.release(reclaimedSendPermits);
                        break;
                    }
                } else {
                    sendBudgetSemaphore.release(reclaimedSendPermits);
                    break;
                }
            }

            // Drain pending recvs queue
            while (recvBudgetSemaphore != null && !isStopped.get() && !isError()) {
                PendingReceive pendingReceive = recvWrQueue.poll();
                if (pendingReceive != null) {
                    // If there are not enough available permits from
                    // this run AND from the semaphore, then it means that there are
                    // more completions coming and they will exhaust the queue later
                    if (pendingReceive.ibvRecvWRList.size() > reclaimedRecvWrs) {
                        if (!recvBudgetSemaphore.tryAcquire(pendingReceive.ibvRecvWRList.size() - reclaimedRecvWrs)) {
                            recvWrQueue.push(pendingReceive);
                            recvBudgetSemaphore.release(reclaimedRecvWrs);
                            break;
                        } else {
                            reclaimedRecvWrs = 0;
                        }
                    } else {
                        reclaimedRecvWrs -= pendingReceive.ibvRecvWRList.size();
                    }

                    try {
                        rdmaPostRecvWRList(pendingReceive.ibvRecvWRList);
                    } catch (IOException e) {
                        setRdmaChannelState(RdmaChannelState.ERROR);
                        // reclaim the credit and put sendWRList back to the queue
                        // however, the channel/QP is already broken and more actions
                        // needed to be taken to recover
                        reclaimedRecvWrs += pendingReceive.ibvRecvWRList.size();

                        recvWrQueue.push(pendingReceive);
                        recvBudgetSemaphore.release(reclaimedRecvWrs);
                        break;
                    }
                } else {
                    recvBudgetSemaphore.release(reclaimedRecvWrs);
                    break;
                }
            }
        }
    }

    boolean processCompletions() throws IOException {
        // Disni's API uses a CQ here, which is wrong
        boolean success = compChannel.getCqEvent(cq, 50);
        if (success) {
            ackCounter ++;
            if (ackCounter == MAX_ACK_COUNT) {
                cq.ackEvents(ackCounter);
                ackCounter = 0;
            }

            if (!isStopped.get()) {
                reqNotifyCall.execute();
            }

            exhaustCq();

            return true;
        } else if (isStopped.get() && ackCounter > 0) {
            cq.ackEvents(ackCounter);
            ackCounter = 0;
        }

        return false;
    }

    public void stop() throws InterruptedException, IOException {
        if (!isStopped.getAndSet(true)) {
            LOG.info("Stopping RdmaChannel " + this);

            if (rdmaThread != null) rdmaThread.stop();

            // Fail pending completionInfos
            for (Integer completionInfoId: completionInfoMap.keySet()) {
                final CompletionInfo completionInfo = completionInfoMap.remove(completionInfoId);
                if (completionInfo != null) {
                    completionInfo.listener.onFailure(
                            new IOException("RDMA Send/Read WR revoked since QP was removed"));
                }
            }

            if (cmId != null) {
                int ret = cmId.disconnect();
                if (ret != 0) {
                    LOG.error("disconnect failed with errno: " + ret);
                } else if (rdmaChannelType.equals(RdmaChannelType.RPC) ||
                        rdmaChannelType.equals(RdmaChannelType.RDMA_READ_REQUESTOR)) {
                    try {
                        processRdmaCmEvent(RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal(),
                                teardownListenTimeout);
                    } catch (IOException e) {
                        LOG.warn("Failed to get RDMA_CM_EVENT_DISCONNECTED: " + e.getLocalizedMessage());
                    }
                }

                if (qp != null) {
                    ret = cmId.destroyQP();
                    if (ret != 0) {
                        LOG.error("destroyQP failed with errno: " + ret);
                    }
                }
            }

            if (reqNotifyCall != null) {
                reqNotifyCall.free();
            }

            if (svcPollCq != null) {
                svcPollCq.free();
            }

            if (cq != null) {
                if (ackCounter > 0) {
                    cq.ackEvents(ackCounter);
                }
                int ret = cq.destroyCQ();
                if (ret != 0) {
                    LOG.error("destroyCQ failed with errno: " + ret);
                }
            }

            if (cmId != null) {
                int ret = cmId.destroyId();
                if (ret != 0) {
                    LOG.error("destroyId failed with errno: " + ret);
                }
            }

            if (compChannel != null) {
                int ret = compChannel.destroyCompChannel();
                if (ret != 0) {
                    LOG.error("destroyCompChannel failed with errno: " + ret);
                }
            }

            if (eventChannel != null) {
                int ret = eventChannel.destroyEventChannel();
                if (ret != 0) {
                    LOG.error("destroyEventChannel failed with errno: " + ret);
                }
            }
        }
    }

    InetSocketAddress getSourceSocketAddress() throws IOException {
        return (InetSocketAddress)cmId.getSource();
    }

    InetSocketAddress getDestinationAddress() throws IOException {
        return (InetSocketAddress)cmId.getDestination();
    }
    private void processRdmaCmEvent(int expectedEvent, int timeout) throws IOException {
        if(eventChannel == null) return;

        RdmaCmEvent event = eventChannel.getCmEvent(timeout);
        if (event == null) {
            setRdmaChannelState(RdmaChannelState.ERROR);
            throw new IOException("getCmEvent() failed");
        }

        int eventType = event.getEvent();
        event.ackEvent();

        if (eventType != expectedEvent) {
            setRdmaChannelState(RdmaChannelState.ERROR);
            throw new IOException("Received CM event: " + RdmaCmEvent.EventType.values()[eventType] + " but expected: " + RdmaCmEvent.EventType.values()[expectedEvent]);
        }
    }

    public boolean isConnected() { return rdmaChannelState.get() == RdmaChannelState.CONNECTED.ordinal(); }
    public boolean isError() { return rdmaChannelState.get() == RdmaChannelState.ERROR.ordinal(); }

    private static class CompletionInfo {
        final RdmaCompletionListener listener;
        final int sendPermitsToReclaim;
        CompletionInfo(RdmaCompletionListener rdmaCompletionListener, int sendPermitsToReclaim) {
            this.listener = rdmaCompletionListener;
            this.sendPermitsToReclaim = sendPermitsToReclaim;
        }
    }




    // Send a SendWR
    private class PendingSend {
        final LinkedList<IbvSendWR> ibvSendWRList;
        final int recvCreditsNeeded;

        PendingSend(LinkedList<IbvSendWR> ibvSendWRList, int recvCreditsNeeded) {
            this.ibvSendWRList = ibvSendWRList;
            this.recvCreditsNeeded = recvCreditsNeeded;
        }
    }

    // Receive a ReceiveWR
    private class PendingReceive {
        final LinkedList<IbvRecvWR> ibvRecvWRList;
        final int recvCreditsNeeded;

        PendingReceive(LinkedList<IbvRecvWR> ibvRecvWRList, int recvCreditsNeeded) {
            this.ibvRecvWRList = ibvRecvWRList;
            this.recvCreditsNeeded = recvCreditsNeeded;
        }
    }
    private class PostRecvWr {
        final IbvRecvWR ibvRecvWR;
        final RdmaBuffer rdmaBuf;
        final ByteBuffer buf;

        PostRecvWr(IbvRecvWR ibvRecvWR, RdmaBuffer rdmaBuf) throws IOException {
            this.ibvRecvWR = ibvRecvWR;
            this.rdmaBuf = rdmaBuf;
            this.buf = rdmaBuf.getByteBuffer();
        }
    }
}