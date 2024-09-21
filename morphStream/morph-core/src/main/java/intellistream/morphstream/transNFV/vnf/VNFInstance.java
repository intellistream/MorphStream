package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.transNFV.common.Operation;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import static intellistream.morphstream.transNFV.vnf.UDF.executeUDF;

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

    private final ConcurrentHashMap<Integer, Integer> tupleUnderCCSwitch = new ConcurrentHashMap<>(); // Affected tupleID -> new CC
    private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<VNFRequest>> tupleBufferReqMap = new ConcurrentHashMap<>();
    private final HashMap<Integer, Long> bufferReqStartTimeMap = new HashMap<>();
//    private final BlockingQueue<Integer> monitorMsgQueue = new LinkedBlockingQueue<>(); //Workload monitor controls instance punctuation barriers

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
    private final int patternPunctuation = MorphStreamEnv.get().configuration().getInt("instancePatternPunctuation");
    private long aggParsingTime = 0; // ccID -> total parsing time
    private long AGG_SYNC_TIME = 0; // ccID -> total sync time at instance, for CC with local sync
    private long AGG_USEFUL_TIME = 0; // ccID -> total useful time at instance, for CC with local RW
    private long aggCCSwitchTime = 0; // ccID -> total time for CC switch


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
            for (int i = 0; i <= stateRange; i++) {
                tupleCCMap.put(i, "Partitioning");
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


    public void addTupleCCSwitch(int tupleID, int newCC) {
        tupleUnderCCSwitch.put(tupleID, newCC);
    }

//    public void notifyNextPuncStart(int nextPuncID) {
//        try {
////            monitorMsgQueue.put(nextPuncID);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public void endTupleCCSwitch(int tupleID, String newCC) {
        tupleUnderCCSwitch.remove(tupleID);
        tupleCCMap.put(tupleID, newCC);
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
                VNFRequest request = createRequest(line);
                TXN_PROCESS(request);
            }

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

        } catch (IOException | InterruptedException | BrokenBarrierException e) {
            System.err.println("Error reading from file: " + e.getMessage());

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
        int saID = 0; //TODO: Hardcoded saID
        return new VNFRequest(reqID, instanceID, tupleID, 0, scope, type, vnfID, saID, packetStartTime, instancePuncID);
    }


    private void TXN_PROCESS(VNFRequest request) throws InterruptedException {
        int reqID = request.getReqID();
        int tupleID = request.getTupleID();
        String type = request.getType();
        String scope = request.getScope();
        String tupleCC = tupleCCMap.get(tupleID);
        boolean involveWrite = Objects.equals(type, "Write") || Objects.equals(type, "Read-Write");

        if (Objects.equals(scope, "Per-flow")) { // Per-flow operations, no need to acquire lock. CANNOT be executed together with cross-flow operations
            localSVCCStateManager.executeTransaction(request);
            submitFinishedRequest(request);
            return;
        }

        if (Objects.equals(tupleCC, "Partitioning")) {
            if (statePartitionStart <= tupleID && tupleID <= statePartitionEnd) {
                localSVCCStateManager.executeTransaction(request);
                submitFinishedRequest(request);
            }
            else {
                partitionStateManager.submitPartitioningRequest(request);
                while (true) {
                    VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
            }
//            if (statePartitionStart <= tupleID && tupleID <= statePartitionEnd) {
//                localSVCCStateManager.executeTransaction(request);
//            }
//            else {
//                int targetInstanceID = VNFManager.getPartitionedInstanceID(tupleID);
//                LocalSVCCStateManager remoteStateManager = VNFManager.getInstanceStateManager(targetInstanceID);
//                executeSVCCTransaction(remoteStateManager, request); // Direct R/W to remote instance
//            }
//            submitFinishedRequest(request);

        } else if (Objects.equals(tupleCC, "Replication")) { // Replication: async read, sync write
            localSVCCStateManager.executeTransaction(request);
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

//            if (involveWrite) { // hasWrite: broadcast updates to all other instances
////                timeout(100); // Simulate network delay
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
            offloadingQueues.get(offloadRequestCount % numOffloadThreads).offer(request);
            offloadRequestCount++;
//            if (!Objects.equals(request.getType(), "Write")) { //Simply continue with the next request without waiting for ACK
//                while (responseQueue.isEmpty()) {
//                    //Wait for manager's ack
//                }
//            }

        } else if (Objects.equals(tupleCC, "Proactive")) { // Preemptive
            tpgInputQueues.get(tpgRequestCount % numTPGThreads).offer(new ProactiveVNFRequest(request));
            tpgRequestCount++;

        } else if (Objects.equals(tupleCC, "OpenNF")) { // OpenNF
            openNFStateManager.submitOpenNFReq(request);
            while (true) {
                VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }

        } else if (Objects.equals(tupleCC, "CHC")) { // CHC
            chcStateManager.submitCHCReq(request);
            while (true) {
                VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }

        } else if (Objects.equals(tupleCC, "S6")) { // S6
            if (Objects.equals(type, "Read")) { // read
                if (!pendingStateSyncs.isEmpty()) {
                    applyStateSync();
                }
                executeUDF(request);
                submitFinishedRequest(request);

            } else if (involveWrite) { // write
                //TODO: Remove replication manager. This instance should wait for all other instances' ACK before proceeding
                executeUDF(request);
                s6StateManager.submitS6Request(request);
                while (true) {
                    VNFRequest lastFinishedReq = blockingFinishedReqs.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
            }

        } else {
            throw new UnsupportedOperationException("Unsupported CC strategy: " + tupleCC);
        }
    }

    private void applyStateSync() throws InterruptedException {
        while (!pendingStateSyncs.isEmpty()) {
            SyncData data = pendingStateSyncs.take();  // Block if necessary until an item is available
            int tupleID = data.getTupleID();
            int value = data.getValue();
            localSVCCStateManager.nullSafeStateUpdate(tupleID, value);
        }
    }

    public void addStateSync(SyncData data) {
        try {
            pendingStateSyncs.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeSVCCTransaction(LocalSVCCStateManager stateManager, VNFRequest request) throws InterruptedException {
        int tupleID = request.getTupleID();
        String type = request.getType();
        long timestamp = request.getCreateTime();

        Transaction transaction = constructTransaction(request, type, tupleID, timestamp);

        // Phase 1: Acquire all locks
        Set<Integer> sharedLocks = new HashSet<>();
        Set<Integer> exclusiveLocks = new HashSet<>();
        for (Operation operation : transaction.getOperations()) {
            if (operation.isWrite()) {
                exclusiveLocks.add(operation.getKey());
            } else {
                sharedLocks.add(operation.getKey());
            }
        }

        sharedLocks.removeAll(exclusiveLocks);
        for (int key : sharedLocks) {
            stateManager.acquireLock(key, transaction.getTimestamp(), false);
            transaction.getAcquiredLocks().add(key);
        }
        for (int key : exclusiveLocks) {
            stateManager.acquireLock(key, transaction.getTimestamp(), true);
            transaction.getAcquiredLocks().add(key);
        }

        // Simulate transaction execution
        stateManager.executeTransaction(request);

        // Phase 2: Release all locks
        for (int key : transaction.getAcquiredLocks()) {
            stateManager.releaseLock(key, timestamp);
        }
    }

    private void syncSVCCStateUpdate(LocalSVCCStateManager stateManager, VNFRequest request) throws InterruptedException {
        int tupleID = request.getTupleID();
        String type = request.getType();
        long timestamp = request.getCreateTime();

        Transaction transaction = constructTransaction(request, type, tupleID, timestamp);

        // Phase 1: Acquire all locks
        Set<Integer> sharedLocks = new HashSet<>();
        Set<Integer> exclusiveLocks = new HashSet<>();
        for (Operation operation : transaction.getOperations()) {
            if (operation.isWrite()) {
                exclusiveLocks.add(operation.getKey());
            } else {
                sharedLocks.add(operation.getKey());
            }
        }

        sharedLocks.removeAll(exclusiveLocks);
        for (int key : sharedLocks) {
            stateManager.acquireLock(key, transaction.getTimestamp(), false);
            transaction.getAcquiredLocks().add(key);
        }
        for (int key : exclusiveLocks) {
            stateManager.acquireLock(key, transaction.getTimestamp(), true);
            transaction.getAcquiredLocks().add(key);
        }

        // Phase 2: Release all locks
        for (int key : transaction.getAcquiredLocks()) {
            stateManager.releaseLock(key, timestamp);
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
        return aggParsingTime;
    }
    public Long getAGG_SYNC_TIME() {
        return AGG_SYNC_TIME;
    }
    public Long getAGG_USEFUL_TIME() {
        return AGG_USEFUL_TIME;
    }
    public long getAggCCSwitchTime() {
        return aggCCSwitchTime;
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
            throw new UnsupportedOperationException("Adaptive CC strategy not supported yet");
        } else {
            throw new UnsupportedOperationException("Unsupported CC strategy: " + ccStrategy);
        }
    }
}
