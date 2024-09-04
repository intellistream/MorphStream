package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.ProactiveVNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.adaptation.BatchMonitorThread;
import intellistream.morphstream.transNFV.common.PatternData;
import intellistream.morphstream.transNFV.common.SyncData;
import intellistream.morphstream.transNFV.state_managers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.*;

import static intellistream.morphstream.transNFV.vnf.UDF.executeUDF;

public class VNFInstanceNew implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(VNFInstanceNew.class);
    private int instanceID;
    private String csvFilePath;
    private final String ccStrategy;
    private final int statePartitionStart;
    private final int statePartitionEnd;
    private final CyclicBarrier finishBarrier;

    private final HashMap<Integer, Integer> localStates = new HashMap<>(); //TODO: To be refined
    private final ConcurrentHashMap<Integer, String> tupleCCMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgInputQueues = UniversalStateManager.tpgQueues;
    private ConcurrentHashMap<Integer, BlockingQueue<VNFRequest>> offloadingQueues = UniversalStateManager.offloadingQueues;

    private final BlockingQueue<SyncData> managerStateSyncQueue = new LinkedBlockingQueue<>(); //TODO: Pending state updates from other replications
    private final BlockingQueue<VNFRequest> tempFinishedReqQueue = new LinkedBlockingQueue<>(); //TODO: Waiting for manager's ACK for state update broadcast finish
    private final ConcurrentLinkedDeque<VNFRequest> finishedReqStorage = new ConcurrentLinkedDeque<>(); //Permanent storage of finished requests

    private final ConcurrentHashMap<Integer, Integer> tupleUnderCCSwitch = new ConcurrentHashMap<>(); // Affected tupleID -> new CC
    private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<VNFRequest>> tupleBufferReqMap = new ConcurrentHashMap<>();
    private final HashMap<Integer, Long> bufferReqStartTimeMap = new HashMap<>();
    private final BlockingQueue<Integer> monitorMsgQueue = new LinkedBlockingQueue<>(); //TODO: Workload monitor controls instance punctuation barriers


    private int instancePuncID = 1; //Start from 1
    private final int numTPGThreads;
    private final int numOffloadThreads = MorphStreamEnv.get().configuration().getInt("offloadCCThreadNum");
    private int tpgRequestCount = 0;
    private int inputLineCounter = 0;
    private long overallStartTime;
    private long overallEndTime;
    private final int expectedRequestCount;
    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private final int patternPunctuation = MorphStreamEnv.get().configuration().getInt("instancePatternPunctuation");
    private final boolean enableHardcodeCCSwitch = (MorphStreamEnv.get().configuration().getInt("enableHardcodeCCSwitch") == 1);
    private long aggParsingTime = 0; // ccID -> total parsing time
    private long AGG_SYNC_TIME = 0; // ccID -> total sync time at instance, for CC with local sync
    private long AGG_USEFUL_TIME = 0; // ccID -> total useful time at instance, for CC with local RW
    private long aggCCSwitchTime = 0; // ccID -> total time for CC switch

    public VNFInstanceNew(int instanceID, int statePartitionStart, int statePartitionEnd, int stateRange, String ccStrategy, int numTPGThreads,
                          String csvFilePath, CyclicBarrier finishBarrier, int expectedRequestCount) {
        this.instanceID = instanceID;
        this.statePartitionStart = statePartitionStart;
        this.statePartitionEnd = statePartitionEnd;
        this.ccStrategy = ccStrategy;
        this.numTPGThreads = numTPGThreads;
        this.csvFilePath = csvFilePath;
        this.finishBarrier = finishBarrier;
        this.expectedRequestCount = expectedRequestCount;
        for (int i = 0; i <= stateRange; i++) {
            int stateDefaultValue = 0;
            localStates.put(i, stateDefaultValue);
        }
        if (ccStrategy == "Adaptive") { // Adaptive CC started from default CC strategy - Partitioning
            if (enableHardcodeCCSwitch) {
                for (int i = 0; i <= stateRange; i++) {
                    tupleCCMap.put(i, "Partitioning");
                }
            } else {
                for (int i = 0; i <= stateRange; i++) {
                    tupleCCMap.put(i, "Partitioning");
                }
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
            tempFinishedReqQueue.put(request); // This is dynamically pushed and polled
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void addTupleCCSwitch(int tupleID, int newCC) {
        tupleUnderCCSwitch.put(tupleID, newCC);
    }

    public void notifyNextPuncStart(int nextPuncID) {
        try {
            monitorMsgQueue.put(nextPuncID);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void endTupleCCSwitch(int tupleID, String newCC) {
        tupleUnderCCSwitch.remove(tupleID);
        tupleCCMap.put(tupleID, newCC);
    }

    @Override
    public void run() {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(csvFilePath));
            //TODO: Control input arrival rate
            String line;
            overallStartTime = System.nanoTime();
            while ((line = reader.readLine()) != null) {

                long packetStartTime = System.nanoTime();
                long parsingStartTime = System.nanoTime();
                String[] parts = line.split(",");
                int reqID = Integer.parseInt(parts[0]);
                int tupleID = Integer.parseInt(parts[1]);
                int vnfID = Integer.parseInt(parts[2]);
                String type = parts[3]; // 0: read, 1: write, 2: read-write
                String scope = parts[4]; // 0: per-flow, 1: cross-flow
                int saID = 0; //TODO: Hardcoded saID

                VNFRequest request = new VNFRequest(reqID, instanceID, tupleID, 0, scope, type, vnfID, saID, packetStartTime, instancePuncID);
                inputLineCounter++;
                if (enableTimeBreakdown) {
                    aggParsingTime += System.nanoTime() - parsingStartTime;
                }

                if (ccStrategy == "Adaptive") {
//                    BatchMonitorThread.submitPatternData(new PatternData(instanceID, tupleID, type));
                }
                if (ccStrategy == "Adaptive" && tupleUnderCCSwitch.containsKey(tupleID)) {
                    tupleBufferReqMap.computeIfAbsent(tupleID, k -> new ConcurrentLinkedQueue<>()).add(request); // Buffer affected tuples
                    bufferReqStartTimeMap.put(reqID, System.nanoTime());
                } else {
                    txnProcess(request);
                }

                // TODO: Optimization: Periodically check for buffered inputs to be released
                for (Integer tupleId : tupleBufferReqMap.keySet()) {
                    if (!tupleUnderCCSwitch.containsKey(tupleId)) {
                        for (VNFRequest bufferedRequest : tupleBufferReqMap.get(tupleId)) {
                            long bufferReqStartTime = bufferReqStartTimeMap.get(bufferedRequest.getReqID());
                            if (enableTimeBreakdown) {
                                aggCCSwitchTime += System.nanoTime() - bufferReqStartTime;
                            }
                            txnProcess(bufferedRequest);
                        }
                        tupleBufferReqMap.remove(tupleId);
                    }
                }

                if (inputLineCounter % patternPunctuation == 0) {
                    instancePuncID++; // Start from 1
                    //TODO: Either refine, or remove hardcoded strategy switching

                    if (ccStrategy == "Adaptive") {
                        long ccSwitchStartTime = System.nanoTime();
                        int nextPuncID = monitorMsgQueue.take(); // Wait for pattern monitor's signal to begin next punctuation
                        assert nextPuncID == instancePuncID;
                        if (enableTimeBreakdown) {
                            aggCCSwitchTime += System.nanoTime() - ccSwitchStartTime;
                        }
                        LOG.info("Instance " + instanceID + " starts punctuation " + instancePuncID);
//                        if (enableHardcodeCCSwitch) {
//                            if (inputLineCounter == 100000) {
//                                tupleCCMap.replaceAll((k, v) -> 1);
//                                System.out.println("Instance " + instanceID + " switches to cc " + tupleCCMap.get(1));
//                            }
//                        }
                    }
                }
            }

            System.out.println("Instance " + instanceID + " finished parsing " + inputLineCounter + " requests.");
            assert inputLineCounter == expectedRequestCount;

            while (finishedReqStorage.size() < expectedRequestCount) {
                Thread.sleep(1000);
            }

            // Compute instance local performance
            overallEndTime = finishedReqStorage.peekLast().getFinishTime();
            long overallDuration = overallEndTime - overallStartTime;
            System.out.println("Instance " + instanceID + " processed all " + expectedRequestCount + " requests, Throughput " + (expectedRequestCount / (overallDuration / 1E9)) + " events/sec");

            int arrivedIndex = finishBarrier.await(); // Wait for other instances to finish
            if (arrivedIndex == 0) {
                System.out.println("All instances have finished, sending stop signals to StateManager...");

                VNFRequest stopSignal = new VNFRequest(-1, -1, -1, -1, "-1", "-1", -1, -1, -1, -1);
//                BatchMonitorThread.submitPatternData(new PatternData(-1, 0, "-1"));
                PartitionStateManager.submitPartitionRequest(stopSignal);
                ReplicationStateManager.submitReplicationRequest(stopSignal);
                for (int offloadQueueIndex = 0; offloadQueueIndex < offloadingQueues.size(); offloadQueueIndex++) {
                    offloadingQueues.get(offloadQueueIndex).offer(new VNFRequest(-1, -1, -1, -1, "-1", "-1", -1, -1, -1, -1));
                }
                OpenNFStateManager.submitOpenNFReq(stopSignal);
                CHCStateManager.submitCHCReq(stopSignal);
                S6StateManager.submitS6Request(stopSignal);
                for (int tpgQueueIndex = 0; tpgQueueIndex < tpgInputQueues.size(); tpgQueueIndex++) {
                    tpgInputQueues.get(tpgQueueIndex).offer(new ProactiveVNFRequest(0));
                }
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


    private void txnProcess(VNFRequest request) throws InterruptedException {
        int reqID = request.getReqID();
        int tupleID = request.getTupleID();
        String type = request.getType();
        String scope = request.getScope();
        String tupleCC = tupleCCMap.get(tupleID);

        if (scope == "Per-flow") { // Per-flow operations
            executeUDF(request);
            submitFinishedRequest(request);
            return;
        }

        if (Objects.equals(tupleCC, "Partitioning")) {
            if (statePartitionStart <= tupleID && tupleID <= statePartitionEnd) {
                executeUDF(request);
                submitFinishedRequest(request);

            } else {
                //TODO: Remove partitioning manager. Partition thread should perform cross-partition access by itself
                PartitionStateManager.submitPartitionRequest(request);
                while (true) {
                    VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
            }

        } else if (Objects.equals(tupleCC, "Replication")) { // Replication
            if (type == "Read") { // read-only
                if (!managerStateSyncQueue.isEmpty()) { // TODO: Sync pending state updates from other instances
                    processSyncQueue();
                }

                executeUDF(request);
                submitFinishedRequest(request);

            } else if (type == "Write" || type == "Read-Write") { // write
                executeUDF(request);
                ReplicationStateManager.submitReplicationRequest(request);
                while (true) {
                    //TODO: Remove replication manager. This instance should wait for all other instances' ACK before proceeding
                    VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
            }

        } else if (Objects.equals(tupleCC, "Offloading")) {
            BlockingQueue<Integer> responseQueue = new ArrayBlockingQueue<>(1);
            request.setTxnACKQueue(responseQueue);
            offloadingQueues.get(tpgRequestCount % numOffloadThreads).offer(request);
            tpgRequestCount++;
            if (!Objects.equals(request.getType(), "Write")) {
                while (responseQueue.isEmpty()) {
                    //Wait for manager's ack
                }
            }

        } else if (Objects.equals(tupleCC, "Proactive")) { // Preemptive
            tpgInputQueues.get(tpgRequestCount % numTPGThreads).offer(new ProactiveVNFRequest(request));
            tpgRequestCount++;

        } else if (Objects.equals(tupleCC, "OpenNF")) { // OpenNF
            OpenNFStateManager.submitOpenNFReq(request);
            while (true) {
                VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }

        } else if (Objects.equals(tupleCC, "CHC")) { // CHC
            CHCStateManager.submitCHCReq(request);
            while (true) {
                VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }

        } else if (Objects.equals(tupleCC, "S6")) { // S6
            if (type == "Read") { // read

                if (!managerStateSyncQueue.isEmpty()) {
                    processSyncQueue();
                }
                executeUDF(request);
                submitFinishedRequest(request);

            } else if (type == "Write" || type == "Read-Write") { // write

                //TODO: Remove replication manager. This instance should wait for all other instances' ACK before proceeding
                executeUDF(request);
                S6StateManager.submitS6Request(request);
                while (true) {
                    VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
            }

        }
    }

    private void processSyncQueue() throws InterruptedException {
        while (!managerStateSyncQueue.isEmpty()) {
            SyncData data = managerStateSyncQueue.take();  // Block if necessary until an item is available
            int tupleID = data.getTupleID();
            int value = data.getValue();
            writeLocalState(tupleID, value);
        }
    }

    // TODO: For replication, manager broadcast state update to all other instances
    public void submitSyncData(SyncData data) {
        try {
            managerStateSyncQueue.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    public int readLocalState(int tupleID) {
        synchronized (localStates.get(tupleID)) {
            return localStates.get(tupleID);
        }
    }
    public void writeLocalState(int tupleID, int value) {
        synchronized (localStates.get(tupleID)) {
            localStates.put(tupleID, value);
        }
    }


    private int parseScope(String scope) {
        switch (scope) {
            case "per-flow":
                return 0;
            case "cross-flow":
                return 1;
            default:
                return -1;
        }
    }
}
