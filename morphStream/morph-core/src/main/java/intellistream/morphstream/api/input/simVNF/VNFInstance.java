package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class VNFInstance implements Runnable {
    private int instanceID;
    private String csvFilePath;
    private final int ccStrategy;
    private static final ConcurrentHashMap<Integer, Integer> tupleCCMap = new ConcurrentHashMap<>();
    private final int statePartitionStart;
    private final int statePartitionEnd;
    private final int stateRange; //entire state space
    private final CyclicBarrier instancesBarrier;
    private final ConcurrentHashMap<Integer, Integer> localStateMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = AdaptiveCCManager.tpgQueues;
    private final BlockingQueue<SyncData> managerSyncQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<VNFRequest> finishedRequestQueue = new LinkedBlockingQueue<>(); //Communication channel between VNFInstance and StateManager
    private final ConcurrentLinkedDeque<VNFRequest> finishedReqStorage = new ConcurrentLinkedDeque<>(); //Stable storage of finished requests
    private final int numTPGThreads;
    private int tpgRequestCount = 0;
    private int inputLineCounter = 0;
    private long overallStartTime;
    private long overallEndTime;
    private final int expectedRequestCount;

    public VNFInstance(int instanceID, int statePartitionStart, int statePartitionEnd, int stateRange, int ccStrategy, int numTPGThreads,
                       String csvFilePath, CyclicBarrier instancesBarrier, int expectedRequestCount) {
        this.instanceID = instanceID;
        this.statePartitionStart = statePartitionStart;
        this.statePartitionEnd = statePartitionEnd;
        this.stateRange = stateRange;
        this.ccStrategy = ccStrategy;
        this.numTPGThreads = numTPGThreads;
        this.csvFilePath = csvFilePath;
        this.instancesBarrier = instancesBarrier;
        this.expectedRequestCount = expectedRequestCount;
        for (int i = 0; i <= stateRange; i++) {
            int stateDefaultValue = 0;
            localStateMap.put(i, stateDefaultValue);
        }
        if (ccStrategy == 6) { // Adaptive CC started from default CC strategy - Partitioning
            for (int i = 0; i <= stateRange; i++) {
                tupleCCMap.put(i, 0);
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
            finishedRequestQueue.put(request); // This is dynamically pushed and polled
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
                String[] parts = line.split(",");
                int reqID = Integer.parseInt(parts[0]);
                int tupleID = Integer.parseInt(parts[1]);
                int type = Integer.parseInt(parts[2]);

                inputLineCounter++;

                int tupleCC = tupleCCMap.get(tupleID);
                if (tupleCC == 0) { // local state access
                    long timestamp = System.nanoTime();
                    if (tupleID >= statePartitionStart && tupleID <= statePartitionEnd) {
                        executeUDF(tupleID, type, 0);
                        submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));

                    } else { // cross-partition state access
                        PartitionCCThread.submitPartitionRequest(new PartitionData(timestamp, reqID, instanceID, tupleID, 0, -1));
                        while (true) {
                            VNFRequest lastFinishedReq = finishedRequestQueue.take();
                            if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                                break;
                            }
                        }
                    }

                } else if (tupleCC == 1) { // Replication
                    long timestamp = System.nanoTime();
                    if (type == 0) { // read
                        if (!managerSyncQueue.isEmpty()) {
                            processAllQueueItems();
                        }
                        executeUDF(tupleID, type, 0);
                        submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));

                    } else if (type == 1 || type == 2) { // write
                        executeUDF(tupleID, type, 0);
                        CacheCCThread.submitReplicationRequest(new CacheData(reqID, timestamp, instanceID, tupleID, 0));
                        while (true) {
                            VNFRequest lastFinishedReq = finishedRequestQueue.take();
                            if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                                break;
                            }
                        }
                    }

                } else if (tupleCC == 2) { // Offload
                    long timestamp = System.nanoTime();
                    OffloadCCThread.submitOffloadReq(new OffloadData(timestamp, instanceID, reqID, tupleID, 0, 0, 0, type));
                    while (true) {
                        VNFRequest lastFinishedReq = finishedRequestQueue.take();
                        if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                            break;
                        }
                    }

                } else if (tupleCC == 3) { // TPG
                    long timestamp = System.nanoTime();
                    tpgQueues.get(tpgRequestCount % numTPGThreads).offer(new TransactionalVNFEvent(type, instanceID, timestamp, reqID, tupleID, 0, 0, 0));
                    tpgRequestCount++;

                } else if (tupleCC == 4) { // OpenNF
                    long timestamp = System.nanoTime();
                    OpenNFController.submitOpenNFReq(new OffloadData(timestamp, instanceID, reqID, tupleID, 0, 0, 0, type));
                    while (true) {
                        VNFRequest lastFinishedReq = finishedRequestQueue.take();
                        if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                            break;
                        }
                    }

                } else if (tupleCC == 5) { // CHC
                    long timestamp = System.nanoTime();
                    CHCController.submitCHCReq(new OffloadData(timestamp, instanceID, reqID, tupleID, 0, 0, 0, type));
                    while (true) {
                        VNFRequest lastFinishedReq = finishedRequestQueue.take();
                        if (lastFinishedReq.getReqID() == reqID) { // Wait for txn_finish from StateManager
                            break;
                        }
                    }

                } else {
                    System.err.println("Unsupported CC strategy");
                }
            }

            System.out.println("Instance " + instanceID + " initiated " + inputLineCounter + " requests.");
            assert inputLineCounter == expectedRequestCount;

            while (finishedReqStorage.size() < expectedRequestCount) {
                Thread.sleep(1000);
            }

            // Compute instance local performance
            overallEndTime = finishedReqStorage.peekLast().getFinishTime();
            long overallDuration = overallEndTime - overallStartTime;
            System.out.println("Instance " + instanceID + " processed all " + expectedRequestCount + " requests, Throughput " + (expectedRequestCount / (overallDuration / 1E9)) + " events/sec");

            int arrivedIndex = instancesBarrier.await(); // Wait for other instances
            if (arrivedIndex == 0) {
                System.out.println("All instances have finished, sending stop signals to StateManager...");

                MonitorThread.submitPatternData(new PatternData(-1, instanceID, 0, false));
                PartitionCCThread.submitPartitionRequest(new PartitionData(-1, 0, instanceID, 0, 0, -1));
                CacheCCThread.submitReplicationRequest(new CacheData(0, -1, 0, instanceID, 0));
                OffloadCCThread.submitOffloadReq(new OffloadData(-1, instanceID, 0, 0, 0, 0, 0, 0));
                OpenNFController.submitOpenNFReq(new OffloadData(-1, instanceID, 0, 0, 0, 0, 0, 0));
                CHCController.submitCHCReq(new OffloadData(-1, instanceID, 0, 0, 0, 0, 0, 0));
                for (int tpgQueueIndex = 0; tpgQueueIndex < tpgQueues.size(); tpgQueueIndex++) {
                    tpgQueues.get(tpgQueueIndex).offer(new TransactionalVNFEvent(0, instanceID, -1, 0, 0, 0, 0, 0));
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

    private int executeUDF(int tupleID, int type, int value) {
        try {
            if (type == 0) {
                return localStateMap.get(tupleID);
            } else if (type == 1) {
                localStateMap.put(tupleID, value);
                return 0;
            } else if (type == 2) {
                int readValue = localStateMap.get(tupleID);
                localStateMap.put(tupleID, readValue);
                return readValue;
            } else {
                return -1;
            }
        } catch (Exception e) {
            System.err.println("Error in VNF function: " + e.getMessage());
            return -1;
        }
    }

    private void processAllQueueItems() throws InterruptedException {
        while (!managerSyncQueue.isEmpty()) {
            SyncData data = managerSyncQueue.take();  // Block if necessary until an item is available
            int tupleID = data.getTupleID();
            int value = data.getValue();
            localStateMap.put(tupleID, value);
        }
    }

    public void submitSyncData(SyncData data) {
        try {
            managerSyncQueue.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public int getFinishedRequestCount() {
        return finishedRequestQueue.size();
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
    //TODO: Implement Lock for reading and writing states
    public int readLocalState(int tupleID) {
        return localStateMap.get(tupleID);
    }
    public void writeLocalState(int tupleID, int value) {
        localStateMap.put(tupleID, value);
    }
    public void changeCCStrategy(int tupleID, int newCCStrategy) { //TODO: Implement pause & continue for CC strategy change
        tupleCCMap.put(tupleID, newCCStrategy);
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
