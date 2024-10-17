package intellistream.morphstream.transNFV.executors;

import intellistream.morphstream.transNFV.adaptation.IterativeWorkloadMonitor;
import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.state_managers.*;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LocalExecutor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutor.class);
    private final int localExecutorID;
    BlockingQueue<VNFRequest> inputQueue;
    private int requestCounter;
    private static final int stateRange = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private static final String ccStrategy = MorphStreamEnv.get().configuration().getString("ccStrategy");

    private final PartitionStateManager partitionStateManager = MorphStreamEnv.get().getTransNFVStateManager().getPartitionStateManager();
    private final ReplicationStateManager replicationStateManager = MorphStreamEnv.get().getTransNFVStateManager().getReplicationStateManager();
    private final OpenNFStateManager openNFStateManager = MorphStreamEnv.get().getTransNFVStateManager().getOpenNFStateManager();
    private final CHCStateManager chcStateManager = MorphStreamEnv.get().getTransNFVStateManager().getCHCStateManager();
    private final S6StateManager s6StateManager = MorphStreamEnv.get().getTransNFVStateManager().getS6StateManager();
    private final IterativeWorkloadMonitor workloadMonitor = MorphStreamEnv.get().getTransNFVStateManager().getWorkloadMonitor();

    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private final boolean hardcodeSwitch = (MorphStreamEnv.get().configuration().getInt("hardcodeSwitch") == 1);
    private final int workloadInterval = MorphStreamEnv.get().configuration().getInt("workloadInterval");
    private final int perInstanceWorkloadInterval = workloadInterval / MorphStreamEnv.get().configuration().getInt("numInstances");

    private long usefulStartTime = 0;
    private long syncStartTime = 0;
    private long parsingStartTime = 0;
    private long metadataStartTime = 0; // Metadata collection for adaptive CC

    private long AGG_USEFUL_TIME = 0;
    private long AGG_SYNC_TIME = 0;
    private long AGG_PARSING_TIME = 0;
    private long AGG_SWITCH_TIME = 0; // ccID -> total time for CC switch
    private long AGG_METADATA_TIME = 0; // ccID -> total metadata collection time for adaptive CC

    private static final HashMap<Integer, Integer> instancePartitionStartMap = new HashMap<>();
    private static final HashMap<Integer, Integer> instancePartitionEndMap = new HashMap<>();

    private final ConcurrentHashMap<Integer, String> tupleCCMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> tupleUnderCCSwitch = new ConcurrentHashMap<>(); // Tuples under CC switch, buffer its future requests to txn buffer
    private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<VNFRequest>> tupleBufferReqMap = new ConcurrentHashMap<>();
    private final HashMap<Long, Long> requestBufferStartTimeMap = new HashMap<>();

    public LocalExecutor(int localExecutorID, BlockingQueue<VNFRequest> inputQueue,
                         HashMap<Integer, Integer> instancePartitionStartMap, HashMap<Integer, Integer> instancePartitionEndMap) {
        this.localExecutorID = localExecutorID;
        this.inputQueue = inputQueue;
        this.requestCounter = 0;
        this.instancePartitionStartMap.putAll(instancePartitionStartMap);
        this.instancePartitionEndMap.putAll(instancePartitionEndMap);

        if (Objects.equals(ccStrategy, "Adaptive")) {
            for (int i = 0; i <= stateRange; i++) {
                tupleCCMap.put(i, "Partitioning");
            }

        } else if (Objects.equals(ccStrategy, "Nested")) { // Exp 5.2.3 fine-grained workload variation
            for (int i = 0; i < stateRange/2; i++) {
                tupleCCMap.put(i, "Partitioning");
            }
            for (int i = stateRange/2; i <= stateRange; i++) {
                tupleCCMap.put(i, "Offloading");
            }

        } else { // Static CC are fixed throughout the runtime
            for (int i = 0; i <= stateRange; i++) {
                tupleCCMap.put(i, ccStrategy);
            }
        }
    }

    @Override
    public void run() {

        System.out.println("Local executor " + localExecutorID + " started");

        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;
            try {
                request = inputQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (request.getCreateTime() == -1) {
                System.out.println("Local executor " + localExecutorID + " received stop signal. Total requests: " + requestCounter);
                break;
            }

            requestCounter++;

            if (ccStrategy.equals("Adaptive")) {
                if (hardcodeSwitch) {
                    if (requestCounter % perInstanceWorkloadInterval == 0) {
                        for (int i = 0; i <= stateRange; i++) {
                            if (tupleCCMap.get(i).equals("Partitioning")) {
                                tupleCCMap.put(i, "Replication");
                            } else if (tupleCCMap.get(i).equals("Replication")) {
                                tupleCCMap.put(i, "Partitioning");
                            }
                        }
                            if (localExecutorID == 0) {
                                LOG.info("Instance 0, current workload interval: " + (requestCounter / perInstanceWorkloadInterval) +
                                        ", switching to: " + tupleCCMap.get(0) + ", total req processed: " + requestCounter);
                            }
                    }
                    TXN_PROCESS(request);

                } else { // The actual adaptive switch based on workload monitoring
                    int tupleID = request.getTupleID();
                    if (isUnderSwitch(tupleID)) {
                        bufferRequest(request);
                    } else {
                        processBufferedRequests(request.getTupleID());
                        TXN_PROCESS(request);
                    }
                }

            } else {
                TXN_PROCESS(request);
            }

        }
    }

    private void TXN_PROCESS(VNFRequest request) {
        int reqID = request.getReqID();
        int tupleID = request.getTupleID();
        String type = request.getType();
        String tupleCC = request.getTupleCC();
        String scope = request.getScope();
        int instanceID = request.getInstanceID();
        boolean involveWrite = Objects.equals(type, "Write") || Objects.equals(type, "Read-Write");

        if (ccStrategy.equals("Adaptive") && (!hardcodeSwitch)) {
            REC_metadataStartTime();
            workloadMonitor.submitMetadata(tupleID, instanceID, type, scope);
            REC_metadataEndTime();
        }

        if (Objects.equals(tupleCC, "Partitioning")) {
            if (instancePartitionStartMap.get(instanceID) <= tupleID && tupleID <= instancePartitionEndMap.get(instanceID)) {
                REC_usefulStartTime();
                VNFManager.getInstanceStateManager(instanceID).nonBlockingTxnExecution(request);
                REC_usefulEndTime();

                REC_parsingStartTime();
                VNFManager.getInstance(instanceID).submitFinishedRequest(request);
                REC_parsingEndTime();

            } else {
                REC_syncStartTime();
                partitionStateManager.submitPartitioningRequest(request);
                while (true) {
                    VNFRequest lastFinishedReq = VNFManager.getInstance(instanceID).checkACK();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
                REC_syncEndTime();

                REC_parsingStartTime();
                VNFManager.getInstance(instanceID).submitFinishedRequest(request);
                REC_parsingEndTime();
            }

        } else if (Objects.equals(tupleCC, "Replication")) { // Replication: async read, sync write
            REC_usefulStartTime();
            VNFManager.getInstanceStateManager(instanceID).nonBlockingTxnExecution(request);
            REC_usefulEndTime();

            REC_syncStartTime();
            if (involveWrite) {
                replicationStateManager.submitReplicationRequest(request);
                while (true) {
                    VNFRequest lastFinishedReq = VNFManager.getInstance(instanceID).checkACK();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
                REC_parsingStartTime();
                VNFManager.getInstance(instanceID).submitFinishedRequest(request);
                REC_parsingEndTime();

            } else {
                try {
                    VNFManager.getInstance(instanceID).applyStateSync();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                REC_parsingStartTime();
                VNFManager.getInstance(instanceID).submitFinishedRequest(request);
                REC_parsingEndTime();
            }
            REC_syncEndTime();

        } else if (Objects.equals(tupleCC, "OpenNF")) { // OpenNF
            openNFStateManager.submitOpenNFReq(request);
            REC_syncStartTime();
            while (true) {
                VNFRequest lastFinishedReq = VNFManager.getInstance(instanceID).checkACK();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }
            REC_syncEndTime();
            VNFManager.getInstance(instanceID).submitFinishedRequest(request);

        } else if (Objects.equals(tupleCC, "S6")) { // S6, always adapts to the replication strategy
            REC_usefulStartTime();
            VNFManager.getInstanceStateManager(instanceID).nonBlockingTxnExecution(request);
            REC_usefulEndTime();

            REC_syncStartTime();
            if (involveWrite) {
                s6StateManager.submitS6Request(request);
                while (true) {
                    VNFRequest lastFinishedReq = VNFManager.getInstance(instanceID).checkACK();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
                REC_parsingStartTime();
                VNFManager.getInstance(instanceID).submitFinishedRequest(request);
                REC_parsingEndTime();

            } else {
                try {
                    VNFManager.getInstance(instanceID).applyStateSync();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                REC_parsingStartTime();
                VNFManager.getInstance(instanceID).submitFinishedRequest(request);
                REC_parsingEndTime();
            }
            REC_syncEndTime();

        } else if (Objects.equals(tupleCC, "CHC")) { // CHC
            chcStateManager.submitCHCReq(request);
            REC_syncStartTime();
            while (true) {
                VNFRequest lastFinishedReq = VNFManager.getInstance(instanceID).checkACK();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }
            if (request.proceedCHCLocalExecution()) { // Do local txn execution if CHC Manager allows, otherwise txn is done by CHC Manager already
                REC_usefulStartTime();
                VNFManager.getInstanceStateManager(instanceID).nonBlockingTxnExecution(request);
                REC_usefulEndTime();
            }
            VNFManager.getInstance(instanceID).submitFinishedRequest(request);
            REC_syncEndTime();

        }
    }


    public void startTupleCCSwitch(int tupleID, String newCC) {
        tupleUnderCCSwitch.put(tupleID, newCC);
    }

    public void endTupleCCSwitch(int tupleID, String newCC) {
        tupleCCMap.put(tupleID, newCC);
        tupleUnderCCSwitch.remove(tupleID);
    }

    private boolean isUnderSwitch(int tupleID) {
        return tupleUnderCCSwitch.containsKey(tupleID);
    }

    private void bufferRequest(VNFRequest request) {
        tupleBufferReqMap.computeIfAbsent(request.getTupleID(), k -> new ConcurrentLinkedQueue<>()).add(request);
        requestBufferStartTimeMap.put(request.getCreateTime(), System.nanoTime());
    }

    private void processBufferedRequests(int tupleID) {
        ConcurrentLinkedQueue<VNFRequest> bufferQueue = tupleBufferReqMap.get(tupleID);
        if (bufferQueue != null) {
            while (!bufferQueue.isEmpty()) {
                VNFRequest bufferedRequest = bufferQueue.poll();
                AGG_SWITCH_TIME += System.nanoTime() - requestBufferStartTimeMap.get(bufferedRequest.getCreateTime());
                TXN_PROCESS(bufferedRequest);
            }
        }
    }

    private void REC_usefulStartTime() {
        if (enableTimeBreakdown) {
            usefulStartTime = System.nanoTime();
        }
    }

    private void REC_usefulEndTime() {
        if (enableTimeBreakdown) {
            AGG_USEFUL_TIME += System.nanoTime() - usefulStartTime;
        }
    }

    private void REC_parsingStartTime() {
        if (enableTimeBreakdown) {
            parsingStartTime = System.nanoTime();
        }
    }

    private void REC_parsingEndTime() {
        if (enableTimeBreakdown) {
            AGG_PARSING_TIME += System.nanoTime() - parsingStartTime;
        }
    }

    private void REC_syncStartTime() {
        if (enableTimeBreakdown) {
            syncStartTime = System.nanoTime();
        }
    }

    private void REC_syncEndTime() {
        if (enableTimeBreakdown) {
            AGG_SYNC_TIME += System.nanoTime() - syncStartTime;
        }
    }

    public long getAGG_USEFUL_TIME() {
        return AGG_USEFUL_TIME;
    }

    public long getAGG_PARSING_TIME() {
        return AGG_PARSING_TIME;
    }

    public long getAGG_SYNC_TIME() {
        return AGG_SYNC_TIME;
    }

    public long getAGG_CC_SWITCH_TIME() {
        return AGG_SWITCH_TIME;
    }

    private void REC_metadataStartTime() {
        if (enableTimeBreakdown) {
            metadataStartTime = System.nanoTime();
        }
    }

    private void REC_metadataEndTime() {
        if (enableTimeBreakdown) {
            AGG_METADATA_TIME += System.nanoTime() - metadataStartTime;
        }
    }

    public long getAGG_METADATA_TIME() {
        return AGG_METADATA_TIME;
    }


    public int getFinishedReqCount() {
        return requestCounter;
    }
}
