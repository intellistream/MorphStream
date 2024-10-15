package intellistream.morphstream.transNFV.executors;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.common.Transaction;
import intellistream.morphstream.transNFV.state_managers.*;
import intellistream.morphstream.transNFV.vnf.LocalSVCCStateManager;
import intellistream.morphstream.transNFV.vnf.VNFManager;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

public class LocalExecutor implements Runnable {
    private final int localExecutorID;
    BlockingQueue<VNFRequest> inputQueue;
    private int requestCounter;
    private static final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private static final String ccStrategy = MorphStreamEnv.get().configuration().getString("ccStrategy");

    private final PartitionStateManager partitionStateManager = MorphStreamEnv.get().getTransNFVStateManager().getPartitionStateManager();
    private final ReplicationStateManager replicationStateManager = MorphStreamEnv.get().getTransNFVStateManager().getReplicationStateManager();
    private final OpenNFStateManager openNFStateManager = MorphStreamEnv.get().getTransNFVStateManager().getOpenNFStateManager();
    private final CHCStateManager chcStateManager = MorphStreamEnv.get().getTransNFVStateManager().getCHCStateManager();
    private final S6StateManager s6StateManager = MorphStreamEnv.get().getTransNFVStateManager().getS6StateManager();

    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private long usefulStartTime = 0;
    private long syncStartTime = 0;
    private long parsingStartTime = 0;
    private long AGG_USEFUL_TIME = 0;
    private long AGG_SYNC_TIME = 0;
    private long AGG_PARSING_TIME = 0;

    private static final HashMap<Integer, Integer> instancePartitionStartMap = new HashMap<>();
    private static final HashMap<Integer, Integer> instancePartitionEndMap = new HashMap<>();

    public LocalExecutor(int localExecutorID, BlockingQueue<VNFRequest> inputQueue,
                         HashMap<Integer, Integer> instancePartitionStartMap, HashMap<Integer, Integer> instancePartitionEndMap) {
        this.localExecutorID = localExecutorID;
        this.inputQueue = inputQueue;
        this.requestCounter = 0;
        this.instancePartitionStartMap.putAll(instancePartitionStartMap);
        this.instancePartitionEndMap.putAll(instancePartitionEndMap);
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

            int reqID = request.getReqID();
            int tupleID = request.getTupleID();
            String type = request.getType();
            String tupleCC = request.getTupleCC();
            int instanceID = request.getInstanceID();
            boolean involveWrite = Objects.equals(type, "Write") || Objects.equals(type, "Read-Write");

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
                    VNFManager.getInstance(instanceID).submitFinishedRequest(request);
                } else {
                    try {
                        VNFManager.getInstance(instanceID).applyStateSync();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    VNFManager.getInstance(instanceID).submitFinishedRequest(request);
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
                    VNFManager.getInstance(instanceID).submitFinishedRequest(request);
                } else {
                    try {
                        VNFManager.getInstance(instanceID).applyStateSync();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    VNFManager.getInstance(instanceID).submitFinishedRequest(request);
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
    }



    private static Transaction constructTransaction(VNFRequest request, String type, int tupleID, long timestamp) {
        Transaction transaction = new Transaction(request.getCreateTime());
        if (Objects.equals(type, "Read")) {
            transaction.addOperation(tupleID, -1, timestamp,false);
        } else if (Objects.equals(type, "Write")) {
            transaction.addOperation(tupleID, -1, timestamp, true);
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + type);
        }
        return transaction;
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

    private void REC_incrementUsefulTime(long usefulTime) {
        if (enableTimeBreakdown) {
            AGG_USEFUL_TIME += usefulTime;
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

    public int getFinishedReqCount() {
        return requestCounter;
    }
}
