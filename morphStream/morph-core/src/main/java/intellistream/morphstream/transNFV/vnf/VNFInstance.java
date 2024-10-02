package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.transNFV.adaptation.IterativeWorkloadMonitor;
import intellistream.morphstream.transNFV.common.Transaction;
import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.ProactiveVNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.common.SyncData;
import intellistream.morphstream.transNFV.state_managers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;


public class VNFInstance implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(VNFInstance.class);
    private final int instanceID;
    private final String csvFilePath;
    private final int statePartitionStart;
    private final int statePartitionEnd;
    private final CyclicBarrier finishBarrier;

    private final PartitionStateManager partitionStateManager = MorphStreamEnv.get().getTransNFVStateManager().getPartitionStateManager();
    private final ReplicationStateManager replicationStateManager = MorphStreamEnv.get().getTransNFVStateManager().getReplicationStateManager();
    private final OpenNFStateManager openNFStateManager = MorphStreamEnv.get().getTransNFVStateManager().getOpenNFStateManager();
    private final CHCStateManager chcStateManager = MorphStreamEnv.get().getTransNFVStateManager().getCHCStateManager();
    private final S6StateManager s6StateManager = MorphStreamEnv.get().getTransNFVStateManager().getS6StateManager();

    private final LocalSVCCStateManager localSVCCStateManager;
    private final ConcurrentHashMap<Integer, String> tupleCCMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgInputQueues = MorphStreamEnv.get().getTransNFVStateManager().tpgInputQueues;
    private final ConcurrentHashMap<Integer, BlockingQueue<VNFRequest>> offloadingQueues = MorphStreamEnv.get().getTransNFVStateManager().offloadInputQueues;

    private final BlockingQueue<SyncData> pendingStateSyncs = new LinkedBlockingQueue<>(); //TODO: Pending state updates from other replications
    private final BlockingQueue<VNFRequest> blockingFinishedReqs = new LinkedBlockingQueue<>(); //TODO: Waiting for manager's ACK for state update broadcast finish
    private final ConcurrentLinkedDeque<VNFRequest> finishedReqStorage = new ConcurrentLinkedDeque<>(); //Permanent storage of finished requests

    private final IterativeWorkloadMonitor workloadMonitor = MorphStreamEnv.get().getTransNFVStateManager().getWorkloadMonitor();
    private final ConcurrentHashMap<Integer, String> tupleUnderCCSwitch = new ConcurrentHashMap<>(); // Tuples under CC switch, buffer its future requests to txn buffer
    private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<VNFRequest>> tupleBufferReqMap = new ConcurrentHashMap<>();

    private String ccStrategy;
    private int instancePuncID = 1; //Start from 1
    private final int numTPGThreads;
    private final int numOffloadThreads = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
    private int tpgRequestCount = 0;
    private int offloadRequestCount = 0;
    private int inputLineCounter = 0;
    private long overallStartTime;
    private long overallEndTime;
    private final int expectedRequestCount;
    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);

    private long parsingStartTime = 0; // Create txn request
    private long syncStartTime = 0; // Ordering, conflict resolution, broadcasting sync
    private long usefulStartTime = 0; // Actual state access and transaction UDF
    private long switchStartTime = 0; // Strategy switching time in TransNFV

    private long AGG_PARSE_TIME = 0; // ccID -> total parsing time
    private long AGG_SYNC_TIME = 0; // ccID -> total sync time at instance, for CC with local sync
    private long AGG_USEFUL_TIME = 0; // ccID -> total useful time at instance, for CC with local RW
    private long AGG_SWITCH_TIME = 0; // ccID -> total time for CC switch


    public VNFInstance(int instanceID, int statePartitionStart, int statePartitionEnd, int stateRange, String ccStrategy, int numTPGThreads,
                          String csvFilePath, LocalSVCCStateManager stateManager, CyclicBarrier finishBarrier, int expectedRequestCount) {
        this.instanceID = instanceID;
        this.ccStrategy = ccStrategy;
        this.statePartitionStart = statePartitionStart;
        this.statePartitionEnd = statePartitionEnd;
        this.numTPGThreads = numTPGThreads;
        this.csvFilePath = csvFilePath;
        this.finishBarrier = finishBarrier;
        this.expectedRequestCount = expectedRequestCount;
        this.localSVCCStateManager = stateManager;
        if (Objects.equals(ccStrategy, "Adaptive")) { // Adaptive CC started from default CC strategy - Partitioning
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


    public void submitFinishedRequest(VNFRequest request) {
        try {
            request.setFinishTime(System.nanoTime());
            finishedReqStorage.add(request);
            blockingFinishedReqs.put(request); // This is dynamically pushed and polled
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void startTupleCCSwitch(int tupleID, String newCC) {
        tupleUnderCCSwitch.put(tupleID, newCC);
    }

    public void endTupleCCSwitch(int tupleID, String newCC) {
//        tupleCCMap.put(tupleID, newCC);
        tupleUnderCCSwitch.remove(tupleID);
    }


    @Override
    public void run() {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(csvFilePath));
            String line;
            overallStartTime = System.nanoTime();
            while ((line = reader.readLine()) != null) {
                inputLineCounter++;

                REC_parsingStartTime();
                VNFRequest request = createRequest(line);
                REC_parsingEndTime();

                if (ccStrategy.equals("Adaptive")) {
                    int tupleID = request.getTupleID();
                    if (!isUnderSwitch(tupleID)) {
                        processBufferedRequests(request.getTupleID());
                        TXN_PROCESS(request); // Main method for txn processing
                    } else {
                        tupleBufferReqMap.computeIfAbsent(request.getTupleID(), k -> new ConcurrentLinkedQueue<>()).add(request);
                    }

                } else {
                    TXN_PROCESS(request); // Main method for txn processing
                }
            }

            waitForTxnFinish();

        } catch (IOException | InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);

        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException ex) {
                System.err.println("Error closing file: " + ex.getMessage());
            }
        }
    }


    private VNFRequest createRequest(String inputLine) {
        long packetStartTime = System.nanoTime();
        String[] parts = inputLine.split(",");
        int reqID = Integer.parseInt(parts[0]);
        int tupleID = Integer.parseInt(parts[1]);
        int vnfID = Integer.parseInt(parts[2]);
        String type = parts[3]; // Read, Write or Read-Write
        String scope = parts[4]; // Per-flow or Cross-flow
        int saID = 0;
        return new VNFRequest(reqID, instanceID, tupleID, 0, scope, type, vnfID, saID, packetStartTime, instancePuncID);
    }


    private boolean isUnderSwitch(int tupleID) {
        return tupleUnderCCSwitch.containsKey(tupleID);
    }

    private void processBufferedRequests(int tupleID) {
        ConcurrentLinkedQueue<VNFRequest> bufferQueue = tupleBufferReqMap.get(tupleID);
        if (bufferQueue != null) {
            while (!bufferQueue.isEmpty()) {
                VNFRequest bufferedRequest = bufferQueue.poll();
                try {
                    TXN_PROCESS(bufferedRequest);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    private void TXN_PROCESS(VNFRequest request) throws InterruptedException {
        int reqID = request.getReqID();
        int tupleID = request.getTupleID();
        String type = request.getType();
        String scope = request.getScope();
        String tupleCC = tupleCCMap.get(tupleID);
        boolean involveWrite = Objects.equals(type, "Write") || Objects.equals(type, "Read-Write");

        if (ccStrategy.equals("Adaptive")) {
            workloadMonitor.submitMetadata(tupleID, instanceID, type, scope);
        }

        if (Objects.equals(scope, "Per-flow")) { // Per-flow operations, no need to acquire lock. CANNOT be executed together with cross-flow operations
            REC_usefulStartTime();
            localSVCCStateManager.nonBlockingTxnExecution(request);
            REC_usefulEndTime();

            REC_parsingStartTime();
            submitFinishedRequest(request);
            REC_parsingEndTime();
            return;
        }

        if (Objects.equals(tupleCC, "Partitioning")) {
            if (statePartitionStart <= tupleID && tupleID <= statePartitionEnd) {
                REC_usefulStartTime();
                localSVCCStateManager.nonBlockingTxnExecution(request);
                REC_usefulEndTime();

                REC_parsingStartTime();
                submitFinishedRequest(request);
                REC_parsingEndTime();
            }
            else {
                REC_syncStartTime();
                partitionStateManager.submitPartitioningRequest(request);
                while (true) {
                    VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
                REC_syncEndTime();
            }
//            else {
//                int targetInstanceID = VNFManager.getPartitionedInstanceID(tupleID);
//                LocalSVCCStateManager remoteStateManager = VNFManager.getInstanceStateManager(targetInstanceID);
//                executeSVCCTransaction(remoteStateManager, request); // Direct R/W to remote instance
//            }
//            submitFinishedRequest(request);

        } else if (Objects.equals(tupleCC, "Replication")) { // Replication: async read, sync write
            REC_usefulStartTime();
            localSVCCStateManager.nonBlockingTxnExecution(request);
            REC_usefulEndTime();

            REC_syncStartTime();
            if (involveWrite) {
                replicationStateManager.submitReplicationRequest(request);
                while (true) {
                    VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
            } else {
                if (!pendingStateSyncs.isEmpty()) {
                    applyStateSync();
                }
                submitFinishedRequest(request);
            }
            REC_syncEndTime();

//            if (involveWrite) { // hasWrite: broadcast updates to all other instances
//                for (int targetInstanceID=0; targetInstanceID<numInstances; targetInstanceID++) {
//                    if (targetInstanceID != instanceID) {
//                        LocalSVCCStateManager remoteStateManager = VNFManager.getInstanceStateManager(targetInstanceID);
//                        syncSVCCStateUpdate(remoteStateManager, request);
//                    }
//                }
//            }
        }

        else if (Objects.equals(tupleCC, "Offloading")) {
//            BlockingQueue<Integer> responseQueue = new ArrayBlockingQueue<>(1);
//            request.setTxnACKQueue(responseQueue);
            REC_syncStartTime();
            offloadingQueues.get(offloadRequestCount % numOffloadThreads).offer(request);
            offloadRequestCount++;
            REC_syncEndTime();
//            if (!Objects.equals(request.getType(), "Write")) { //Simply continue with the next request without waiting for ACK
//                while (responseQueue.isEmpty()) {
//                    //Wait for manager's ack
//                }
//            }

        } else if (Objects.equals(tupleCC, "Proactive")) { // Preemptive
            REC_syncStartTime();
            tpgInputQueues.get(tpgRequestCount % numTPGThreads).offer(new ProactiveVNFRequest(request));
            tpgRequestCount++;
            REC_syncEndTime();

        } else if (Objects.equals(tupleCC, "OpenNF")) { // OpenNF
            openNFStateManager.submitOpenNFReq(request);
            REC_syncStartTime();
            while (true) {
                VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }
            REC_syncEndTime();

        } else if (Objects.equals(tupleCC, "CHC")) { // CHC
            chcStateManager.submitCHCReq(request);
            REC_syncStartTime();
            while (true) {
                VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }
            REC_syncEndTime();

        } else if (Objects.equals(tupleCC, "S6")) { // S6
            REC_usefulStartTime();
            localSVCCStateManager.nonBlockingTxnExecution(request);
            REC_usefulEndTime();

            REC_syncStartTime();
            if (involveWrite) {
                s6StateManager.submitS6Request(request);
                while (true) {
                    VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
            } else {
                if (!pendingStateSyncs.isEmpty()) {
                    applyStateSync();
                }
                submitFinishedRequest(request);
            }
            REC_syncEndTime();

        } else {
            throw new UnsupportedOperationException("Unsupported CC strategy: " + tupleCC);
        }
    }

    private void applyStateSync() throws InterruptedException {
        while (!pendingStateSyncs.isEmpty()) {
            SyncData data = pendingStateSyncs.take();  // Block if necessary until an item is available
            int tupleID = data.getTupleID();
            int value = data.getValue();
            localSVCCStateManager.nonSafeLocalStateUpdate(tupleID, value);
        }
    }

    public void addStateSync(SyncData data) {
        try {
            pendingStateSyncs.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Transaction constructTransaction(VNFRequest request, String type, int tupleID, long timestamp) {
        Transaction transaction = new Transaction(request.getCreateTime());
        if (Objects.equals(type, "Read")) {
            transaction.addOperation(tupleID, -1, timestamp,false);
        } else if (Objects.equals(type, "Write")) {
            transaction.addOperation(tupleID, -1, timestamp, true);
        } else if (Objects.equals(type, "Read-Write")) {
            transaction.addOperation(tupleID, -1, timestamp, false);
            transaction.addOperation(tupleID, -1, timestamp, true);
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + type);
        }
        return transaction;
    }


    /** For performance measurements */


    private void REC_parsingStartTime() {
        if (enableTimeBreakdown) {
            parsingStartTime = System.nanoTime();
        }
    }

    private void REC_parsingEndTime() {
        if (enableTimeBreakdown) {
            AGG_PARSE_TIME += System.nanoTime() - parsingStartTime;
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

    private void REC_switchStartTime() {
        if (enableTimeBreakdown) {
            switchStartTime = System.nanoTime();
        }
    }

    private void REC_switchEndTime() {
        if (enableTimeBreakdown) {
            AGG_SWITCH_TIME += System.nanoTime() - switchStartTime;
        }
    }

    public long[] getPuncStartEndTimes(int puncID) {
        long minStartTime = Long.MAX_VALUE;
        long maxEndTime = Long.MIN_VALUE;
        for (VNFRequest request : this.finishedReqStorage) {
            if (request.getInstancePuncID() == puncID) {
                if (request.getCreateTime() < minStartTime) {
                    minStartTime = request.getCreateTime();
                }
                if (request.getFinishTime() > maxEndTime) {
                    maxEndTime = request.getFinishTime();
                }
            }
        }
        if (minStartTime == Long.MAX_VALUE && maxEndTime == Long.MIN_VALUE) {
            throw new RuntimeException("Error: No requests found for the given punctuation ID");
        }
        return new long[]{minStartTime, maxEndTime};
    }

    public int getFinishedRequestCount() {
        return finishedReqStorage.size();
    }
    public long getOverallStartTime() {
        return overallStartTime;
    }
    public long getOverallEndTime() {
        return overallEndTime;
    }
    public ConcurrentLinkedDeque<VNFRequest> getFinishedReqStorage() {
        return finishedReqStorage;
    }
    public Long getAggParsingTime() {
        return AGG_PARSE_TIME;
    }
    public Long getAGG_SYNC_TIME() {
        return AGG_SYNC_TIME;
    }
    public Long getAGG_USEFUL_TIME() {
        return AGG_USEFUL_TIME;
    }
    public long getAggCCSwitchTime() {
        return AGG_SWITCH_TIME;
    }

    public LocalSVCCStateManager getLocalSVCCStateManager() {
        return localSVCCStateManager;
    }
    private static void timeout(int microseconds) {
        long startTime = System.nanoTime();
        long waitTime = microseconds * 1000L; // Convert microseconds to nanoseconds
        while (System.nanoTime() - startTime < waitTime) {
            // Busy-wait loop
        }
    }

    private void waitForTxnFinish() throws BrokenBarrierException, InterruptedException {
        System.out.println("Instance " + instanceID + " finished parsing " + inputLineCounter + " requests.");
        assert inputLineCounter == expectedRequestCount;
        while (finishedReqStorage.size() < expectedRequestCount) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));  // Sleep for 1 second
        }

        // Compute instance local performance
        assert finishedReqStorage.peekLast() != null;
        overallEndTime = finishedReqStorage.peekLast().getFinishTime();
        long overallDuration = overallEndTime - overallStartTime;
        System.out.println("Instance " + instanceID + " processed all " + expectedRequestCount + " requests, Throughput " + (expectedRequestCount / (overallDuration / 1E9)) + " events/sec");

        int arrivedIndex = finishBarrier.await(); // Wait for other instances to finish
        if (arrivedIndex == 0) {
            System.out.println("All instances have finished, sending stop signals to StateManager...");
            sendStopSignals();
        }
    }

    private void sendStopSignals() {
        VNFRequest stopSignal = new VNFRequest(-1, -1, -1, -1, "-1", "-1", -1, -1, -1, -1);

        if (Objects.equals(ccStrategy, "Partitioning")) {
            partitionStateManager.submitPartitioningRequest(stopSignal);
        } else if (Objects.equals(ccStrategy, "Replication")) {
            replicationStateManager.submitReplicationRequest(stopSignal);
        } else if (Objects.equals(ccStrategy, "Offloading")) {
            for (int offloadQueueIndex = 0; offloadQueueIndex < offloadingQueues.size(); offloadQueueIndex++) {
                offloadingQueues.get(offloadQueueIndex).offer(stopSignal);
            }
        } else if (Objects.equals(ccStrategy, "Proactive")) {
            for (int tpgQueueIndex = 0; tpgQueueIndex < tpgInputQueues.size(); tpgQueueIndex++) {
                tpgInputQueues.get(tpgQueueIndex).offer(new ProactiveVNFRequest(stopSignal));
            }
        } else if (Objects.equals(ccStrategy, "OpenNF")) {
            openNFStateManager.submitOpenNFReq(stopSignal);
        } else if (Objects.equals(ccStrategy, "CHC")) {
            chcStateManager.submitCHCReq(stopSignal);
        } else if (Objects.equals(ccStrategy, "S6")) {
            s6StateManager.submitS6Request(stopSignal);
        } else if (Objects.equals(ccStrategy, "Adaptive")) {
            partitionStateManager.submitPartitioningRequest(stopSignal);
            for (int offloadQueueIndex = 0; offloadQueueIndex < offloadingQueues.size(); offloadQueueIndex++) {
                offloadingQueues.get(offloadQueueIndex).offer(stopSignal);
            }
            workloadMonitor.submitMetadata(-1, -1, "-1", "-1");

        } else {
            throw new UnsupportedOperationException("Unsupported CC strategy: " + ccStrategy);
        }
    }
}
