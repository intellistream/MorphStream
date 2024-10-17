package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.transNFV.adaptation.IterativeWorkloadMonitor;
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


    private final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgInputQueues = MorphStreamEnv.get().getTransNFVStateManager().tpgInputQueues;
    private final ConcurrentHashMap<Integer, BlockingQueue<VNFRequest>> offloadingQueues = MorphStreamEnv.get().getTransNFVStateManager().offloadInputQueues;

    private final BlockingQueue<SyncData> pendingStateSyncs = new LinkedBlockingQueue<>(); //Pending state updates from other replications
    private final BlockingQueue<VNFRequest> blockingFinishedReqs = new LinkedBlockingQueue<>(); //Waiting for manager's ACK for state update broadcast finish
    private final ConcurrentLinkedDeque<VNFRequest> finishedReqStorage = new ConcurrentLinkedDeque<>(); //Permanent storage of finished requests

    private final IterativeWorkloadMonitor workloadMonitor = MorphStreamEnv.get().getTransNFVStateManager().getWorkloadMonitor();
    private final ConcurrentHashMap<Integer, String> tupleCCMap = new ConcurrentHashMap<>();

    private String ccStrategy;
    private int instancePuncID = 1; //Start from 1
    private int stateRange;
    private final int numTPGThreads;
    private final int numOffloadThreads = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
    private int tpgRequestCount = 0;
    private int offloadRequestCount = 0;
    private int inputLineCounter = 0;
    private long overallStartTime;
    private long overallEndTime;
    private final int expectedRequestCount;
    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private final boolean hardcodeSwitch = (MorphStreamEnv.get().configuration().getInt("hardcodeSwitch") == 1);
    private final int workloadInterval = MorphStreamEnv.get().configuration().getInt("workloadInterval");
    private final int perInstanceWorkloadInterval = workloadInterval / MorphStreamEnv.get().configuration().getInt("numInstances");

    private long parsingStartTime = 0; // Create txn request
    private long AGG_PARSE_TIME = 0; // ccID -> total parsing time
    private long AGG_SYNC_TIME = 0; // ccID -> total sync time at instance, for CC with local sync
    private long AGG_USEFUL_TIME = 0; // ccID -> total useful time at instance, for CC with local RW



    public VNFInstance(int instanceID, int statePartitionStart, int statePartitionEnd, int stateRange, String ccStrategy, int numTPGThreads,
                       String csvFilePath, LocalSVCCStateManager localStateManager, CyclicBarrier finishBarrier, int expectedRequestCount) {
        this.instanceID = instanceID;
        this.ccStrategy = ccStrategy;
        this.stateRange = stateRange;
        this.statePartitionStart = statePartitionStart;
        this.statePartitionEnd = statePartitionEnd;
        this.numTPGThreads = numTPGThreads;
        this.csvFilePath = csvFilePath;
        this.finishBarrier = finishBarrier;
        this.expectedRequestCount = expectedRequestCount;
        this.localSVCCStateManager = localStateManager;
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

                forwardTxnToExecutors(request);
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





    private void forwardTxnToExecutors(VNFRequest request) throws InterruptedException {
        int tupleID = request.getTupleID();
        String type = request.getType();
        String scope = request.getScope();
        String tupleCC = tupleCCMap.get(tupleID);
        request.setTupleCC(tupleCC);

        if (Objects.equals(scope, "Per-flow")) {
            localSVCCStateManager.nonBlockingTxnExecution(request);
            submitFinishedRequest(request);
            return;
        }

        if (Objects.equals(tupleCC, "Partitioning")) {
            VNFManager.submitLocalExecutorRequest(request);

        } else if (Objects.equals(tupleCC, "Replication")) { // Replication: async read, sync write
            VNFManager.submitLocalExecutorRequest(request);

        } else if (Objects.equals(tupleCC, "Offloading")) {
            offloadingQueues.get(offloadRequestCount % numOffloadThreads).offer(request);
            offloadRequestCount++;

        } else if (Objects.equals(tupleCC, "Proactive")) { // Preemptive
            tpgInputQueues.get(tpgRequestCount % numTPGThreads).offer(new ProactiveVNFRequest(request));
            tpgRequestCount++;

        } else if (Objects.equals(tupleCC, "OpenNF")) { // OpenNF
            VNFManager.submitLocalExecutorRequest(request);

        } else if (Objects.equals(tupleCC, "CHC")) { // CHC
            VNFManager.submitLocalExecutorRequest(request);

        } else if (Objects.equals(tupleCC, "S6")) { // S6
            VNFManager.submitLocalExecutorRequest(request);

        } else {
            throw new UnsupportedOperationException("Unsupported CC strategy: " + tupleCC);
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


    public void submitFinishedRequest(VNFRequest request) {
        request.setFinishTime(System.nanoTime());
        finishedReqStorage.add(request);
    }

    public void submitACK(VNFRequest request) {
        try {
            blockingFinishedReqs.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public VNFRequest checkACK() {
        try {
            return blockingFinishedReqs.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }




    public void applyStateSync() throws InterruptedException {
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

    public LocalSVCCStateManager getLocalSVCCStateManager() {
        return localSVCCStateManager;
    }

    private void waitForTxnFinish() throws BrokenBarrierException, InterruptedException {
        System.out.println("Instance " + instanceID + " finished parsing " + inputLineCounter + " requests");
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
        MorphStreamEnv.get().vnfExecutionFinished = true;

        VNFRequest stopSignal = new VNFRequest(-1, -1, -1, -1, "-1", "-1", -1, -1, -1, -1);

        if (Objects.equals(ccStrategy, "Partitioning")) {
            VNFManager.stopLocalExecutors();
            partitionStateManager.submitPartitioningRequest(stopSignal);

        } else if (Objects.equals(ccStrategy, "Replication")) {
            VNFManager.stopLocalExecutors();
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
            VNFManager.stopLocalExecutors();
            openNFStateManager.submitOpenNFReq(stopSignal);

        } else if (Objects.equals(ccStrategy, "CHC")) {
            VNFManager.stopLocalExecutors();
            chcStateManager.submitCHCReq(stopSignal);

        } else if (Objects.equals(ccStrategy, "S6")) {
            VNFManager.stopLocalExecutors();
            s6StateManager.submitS6Request(stopSignal);

        } else if (Objects.equals(ccStrategy, "Adaptive")) {
            VNFManager.stopLocalExecutors();
            partitionStateManager.submitPartitioningRequest(stopSignal);
            replicationStateManager.submitReplicationRequest(stopSignal);
            for (int offloadQueueIndex = 0; offloadQueueIndex < offloadingQueues.size(); offloadQueueIndex++) {
                offloadingQueues.get(offloadQueueIndex).offer(stopSignal);
            }
            for (int tpgQueueIndex = 0; tpgQueueIndex < tpgInputQueues.size(); tpgQueueIndex++) {
                tpgInputQueues.get(tpgQueueIndex).offer(new ProactiveVNFRequest(stopSignal));
            }

            workloadMonitor.submitMetadata(-1, -1, "-1", "-1");

        } else if (Objects.equals(ccStrategy, "Nested")) {
            VNFManager.stopLocalExecutors();
            partitionStateManager.submitPartitioningRequest(stopSignal);
            for (int offloadQueueIndex = 0; offloadQueueIndex < offloadingQueues.size(); offloadQueueIndex++) {
                offloadingQueues.get(offloadQueueIndex).offer(stopSignal);
            }

        } else {
            throw new UnsupportedOperationException("Unsupported CC strategy: " + ccStrategy);
        }
    }

}
