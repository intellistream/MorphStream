package intellistream.morphstream.transNFV.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.TransactionalVNFEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.*;
import intellistream.morphstream.transNFV.data.PatternData;
import intellistream.morphstream.transNFV.data.SyncData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.*;

public class VNFInstance implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(VNFInstance.class);
    private int instanceID;
    private String csvFilePath;
    private final int ccStrategy;
    private final int statePartitionStart;
    private final int statePartitionEnd;
    private final int stateRange; //entire state space
    private final CyclicBarrier finishBarrier;
    private final HashMap<Integer, Integer> localStates = new HashMap<>();
    private final ConcurrentHashMap<Integer, Integer> tupleCCMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgInputQueues = AdaptiveCCManager.tpgQueues;
    private ConcurrentHashMap<Integer, BlockingQueue<VNFRequest>> offloadingQueues = AdaptiveCCManager.offloadingQueues;
    private final BlockingQueue<SyncData> managerStateSyncQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<Integer, Integer> tupleUnderCCSwitch = new ConcurrentHashMap<>(); // Affected tupleID -> new CC
    private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<VNFRequest>> tupleBufferReqMap = new ConcurrentHashMap<>();
    private final HashMap<Integer, Long> bufferReqStartTimeMap = new HashMap<>();
    private final BlockingQueue<VNFRequest> tempFinishedReqQueue = new LinkedBlockingQueue<>(); //Communication channel between VNFInstance and StateManager
    private final ConcurrentLinkedDeque<VNFRequest> finishedReqStorage = new ConcurrentLinkedDeque<>(); //Permanent storage of finished requests
    private final BlockingQueue<Integer> monitorMsgQueue = new LinkedBlockingQueue<>();
    private int instancePuncID = 1; //Start from 1
    private final int numTPGThreads;
    private final int numOffloadThreads = MorphStreamEnv.get().configuration().getInt("offloadCCThreadNum");
    private int tpgRequestCount = 0;
    private int offloadRequestCount = 0;
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

    public VNFInstance(int instanceID, int statePartitionStart, int statePartitionEnd, int stateRange, int ccStrategy, int numTPGThreads,
                       String csvFilePath, CyclicBarrier finishBarrier, int expectedRequestCount) {
        this.instanceID = instanceID;
        this.statePartitionStart = statePartitionStart;
        this.statePartitionEnd = statePartitionEnd;
        this.stateRange = stateRange;
        this.ccStrategy = ccStrategy;
        this.numTPGThreads = numTPGThreads;
        this.csvFilePath = csvFilePath;
        this.finishBarrier = finishBarrier;
        this.expectedRequestCount = expectedRequestCount;
        for (int i = 0; i <= stateRange; i++) {
            int stateDefaultValue = 0;
            localStates.put(i, stateDefaultValue);
        }
        if (ccStrategy == 7) { // Adaptive CC started from default CC strategy - Partitioning
            if (enableHardcodeCCSwitch) {
                for (int i = 0; i <= stateRange; i++) {
                    tupleCCMap.put(i, 0);
                }
            } else {
                for (int i = 0; i <= stateRange; i++) {
                    tupleCCMap.put(i, 0);
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

    public void endTupleCCSwitch(int tupleID, int newCC) {
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

                long packetStartTime = System.nanoTime();
                long parsingStartTime = System.nanoTime();
                String[] parts = line.split(",");
                int reqID = Integer.parseInt(parts[0]);
                int tupleID = Integer.parseInt(parts[1]);
                int vnfID = Integer.parseInt(parts[2]);
                int type = Integer.parseInt(parts[3]);
                int saID = type; //TODO: Hardcoded saID = type

                //TODO: Add transaction construction, create transaction based on pre-defined SA structures
                VNFRequest request = new VNFRequest(reqID, instanceID, tupleID, type, packetStartTime, instancePuncID, 0, vnfID, saID);
                inputLineCounter++;
                if (enableTimeBreakdown) {
                    aggParsingTime += System.nanoTime() - parsingStartTime;
                }

                if (ccStrategy == 7) {
                    BatchMonitorThread.submitPatternData(new PatternData(instanceID, tupleID, type));
                }
                if (ccStrategy == 7 && tupleUnderCCSwitch.containsKey(tupleID)) {
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

                    if (ccStrategy == 7) {
                        long ccSwitchStartTime = System.nanoTime();
                        int nextPuncID = monitorMsgQueue.take(); // Wait for pattern monitor's signal to begin next punctuation
                        assert nextPuncID == instancePuncID;
                        if (enableTimeBreakdown) {
                            aggCCSwitchTime += System.nanoTime() - ccSwitchStartTime;
                        }
                        LOG.info("Instance " + instanceID + " starts punctuation " + instancePuncID);
                        if (enableHardcodeCCSwitch) {
                            if (inputLineCounter == 100000) {
                                tupleCCMap.replaceAll((k, v) -> 1);
                                System.out.println("Instance " + instanceID + " switches to cc " + tupleCCMap.get(1));
                            }
                        }
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

            int arrivedIndex = finishBarrier.await(); // Wait for other instances
            if (arrivedIndex == 0) {
                System.out.println("All instances have finished, sending stop signals to StateManager...");

                VNFRequest stopSignal = new VNFRequest(-1, -1, -1, -1, -1, -1, -1, -1, -1);
                BatchMonitorThread.submitPatternData(new PatternData(-1, 0, -1));
                PartitionCCThread.submitPartitionRequest(stopSignal);
                ReplicationCCThread.submitReplicationRequest(stopSignal);
                OffloadCCExecutorService.submitOffloadReq(stopSignal);
                OffloadStateManager.stop();
                for (int offloadQueueIndex = 0; offloadQueueIndex < offloadingQueues.size(); offloadQueueIndex++) {
                    offloadingQueues.get(offloadQueueIndex).offer(new VNFRequest(-1, -1, -1, -1, -1, -1, -1, -1, -1));
                }
                OpenNFController.submitOpenNFReq(stopSignal);
                CHCController.submitCHCReq(stopSignal);
                S6Controller.submitS6Request(stopSignal);
                for (int tpgQueueIndex = 0; tpgQueueIndex < tpgInputQueues.size(); tpgQueueIndex++) {
                    tpgInputQueues.get(tpgQueueIndex).offer(new TransactionalVNFEvent(0, instanceID, -1, 0, 0, 0, 0, -1));
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
        int type = request.getType();
        int tupleCC = tupleCCMap.get(tupleID);

        if (tupleCC == 0) { // local state access
            if (tupleID >= statePartitionStart && tupleID <= statePartitionEnd) {
                long usefulStartTime = System.nanoTime();
                executeUDF(tupleID, type, 0);
                if (enableTimeBreakdown) {
                    AGG_USEFUL_TIME += System.nanoTime() - usefulStartTime;
                }
                submitFinishedRequest(request);

            } else { // cross-partition state access
                //TODO: Partition sync time is measured at manager side, but missing queuing delay
//                long instanceSyncStartTime = System.nanoTime();
                PartitionCCThread.submitPartitionRequest(request);
                while (true) {
                    VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
//                if (enableTimeBreakdown) {
//                    AGG_SYNC_TIME += System.nanoTime() - instanceSyncStartTime;
//                }
            }

        } else if (tupleCC == 1) { // Replication
            if (type == 0) { // read
                long instanceSyncStartTime = System.nanoTime();
                if (!managerStateSyncQueue.isEmpty()) {
                    processSyncQueue();
                }
                if (enableTimeBreakdown) {
                    AGG_SYNC_TIME += System.nanoTime() - instanceSyncStartTime;
                }
                long instanceUsefulStartTime = System.nanoTime();
                executeUDF(tupleID, type, 0);
                if (enableTimeBreakdown) {
                    AGG_USEFUL_TIME += System.nanoTime() - instanceUsefulStartTime;
                }
                submitFinishedRequest(request);

            } else if (type == 1 || type == 2) { // write
                long instanceUsefulStartTime = System.nanoTime();
                executeUDF(tupleID, type, 0);
                if (enableTimeBreakdown) {
                    AGG_USEFUL_TIME += System.nanoTime() - instanceUsefulStartTime;
                }
                long instanceSyncStartTime = System.nanoTime();
                ReplicationCCThread.submitReplicationRequest(request);
                while (true) {
                    VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
                if (enableTimeBreakdown) {
                    AGG_SYNC_TIME += System.nanoTime() - instanceSyncStartTime;
                }
            }

        } else if (tupleCC == 2) { // Offload
            BlockingQueue<Integer> responseQueue = new ArrayBlockingQueue<>(1);
            request.setTxnACKQueue(responseQueue);
            OffloadCCExecutorService.submitOffloadReq(request);
            long syncStartTime = System.nanoTime();
            if (request.getType() != 1) {
                while (responseQueue.isEmpty()) {
                    //Wait for manager's ack
                }
            }
            if (enableTimeBreakdown) {
                AGG_SYNC_TIME += System.nanoTime() - syncStartTime;
            }
//                    System.out.println("Offload response:" + responseQueue.take());
//                    while (true) {
//                        VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
//                        if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
//                            break;
//                        }
//                    }
            // For Offload CC, leave both Sync and Useful time measurement to manager

        } else if (tupleCC == 3) { // Preemptive
            tpgInputQueues.get(tpgRequestCount % numTPGThreads).offer(new TransactionalVNFEvent(type, instanceID, request.getCreateTime(), reqID, tupleID, 0, 0, instancePuncID));
            tpgRequestCount++;
            // For Preemptive CC, leave both Sync and Useful time measurement to manager

        } else if (tupleCC == 4) { // OpenNF
            OpenNFController.submitOpenNFReq(request);
            long syncStartTime = System.nanoTime();
            while (true) {
                VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }
            if (enableTimeBreakdown) {
                AGG_SYNC_TIME += System.nanoTime() - syncStartTime;
            }

        } else if (tupleCC == 5) { // CHC
            CHCController.submitCHCReq(request);
            long syncStartTime = System.nanoTime();
            while (true) {
                VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                    break;
                }
            }
            if (enableTimeBreakdown) {
                AGG_SYNC_TIME += System.nanoTime() - syncStartTime;
            }

        } else if (tupleCC == 6) { // S6
            if (type == 0) { // read
                long instanceSyncStartTime = System.nanoTime();
                if (!managerStateSyncQueue.isEmpty()) {
                    processSyncQueue();
                }
                if (enableTimeBreakdown) {
                    AGG_SYNC_TIME += System.nanoTime() - instanceSyncStartTime;
                }
                long instanceUsefulStartTime = System.nanoTime();
                executeUDF(tupleID, type, 0);
                if (enableTimeBreakdown) {
                    AGG_USEFUL_TIME += System.nanoTime() - instanceUsefulStartTime;
                }
                submitFinishedRequest(request);

            } else if (type == 1 || type == 2) { // write
                long instanceUsefulStartTime = System.nanoTime();
                executeUDF(tupleID, type, 0);
                if (enableTimeBreakdown) {
                    AGG_USEFUL_TIME += System.nanoTime() - instanceUsefulStartTime;
                }
                long instanceSyncStartTime = System.nanoTime();
                S6Controller.submitS6Request(request);
                while (true) {
                    VNFRequest lastFinishedReq = tempFinishedReqQueue.take();
                    if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                        break;
                    }
                }
                if (enableTimeBreakdown) {
                    AGG_SYNC_TIME += System.nanoTime() - instanceSyncStartTime;
                }
            }

        } else if (tupleCC == 10) { //TODO: Testing offload CC with SVCC vs. MVCC
            BlockingQueue<Integer> responseQueue = new ArrayBlockingQueue<>(1);
            request.setTxnACKQueue(responseQueue);
            offloadingQueues.get(tpgRequestCount % numOffloadThreads).offer(request);
            tpgRequestCount++;
//            long syncStartTime = System.nanoTime();
            if (request.getType() != 1) {
                while (responseQueue.isEmpty()) {
                    //Wait for manager's ack
                }
            }
//            if (enableTimeBreakdown) {
//                AGG_SYNC_TIME += System.nanoTime() - syncStartTime;
//            }
        }

        else {
            System.err.println("Unsupported CC strategy");
        }
    }

    private int executeUDF(int tupleID, int type, int value) {
        try {
            if (type == 0) {
                readLocalState(tupleID);
                return 0;
            } else if (type == 1) {
                writeLocalState(tupleID, value);
                return 0;
            } else if (type == 2) {
                int readValue = readLocalState(tupleID);
                int updatedValue = readValue + 1;
                writeLocalState(tupleID, value);
                return updatedValue;
            } else {
                return -1;
            }
        } catch (Exception e) {
            System.err.println("Error in VNF function: " + e.getMessage());
            return -1;
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

    public static void writeIndicatorFile(String fileName) {
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String directoryPath = rootPath + "/indicators";
        String filePath = String.format("%s/%s.csv", directoryPath, fileName);
        System.out.println("Writing indicator file: " + fileName);

        File dir = new File(directoryPath);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return; // Stop further processing if unable to create the directory
            }
        }

        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }

        try {
            file.createNewFile();
        } catch (IOException e) {
            System.out.println("An error occurred while creating the file.");
            e.printStackTrace();
        }
    }
}
