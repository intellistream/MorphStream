package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.*;

public class VNFSenderThread implements Runnable {
    private int instanceID;
    private String csvFilePath;
    private int ccStrategy;
    private int statePartitionStart;
    private int statePartitionEnd;
    private int stateRange; //entire state space
    private int stateDefaultValue = 0;
    private final CyclicBarrier instancesBarrier;
    private final ConcurrentHashMap<Integer, Integer> localStateMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = AdaptiveCCManager.tpgQueues;
    private final BlockingQueue<SyncData> managerSyncQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Integer> managerResponseQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<VNFRequest> finishedRequestQueue = new LinkedBlockingQueue<>();
    private final int numSpouts = MorphStreamEnv.get().configuration().getInt("tthread");
    private int tpgRequestCount = 0;
    private int inputLineCounter = 0;
    private long overallStartTime;
    private long overallEndTime;
    private final int expectedRequestCount;
    private int finishedRequestCount = 0;
    private final ArrayList<Long> latencyList = new ArrayList<>(); //TODO: Sort request order based on requestID, maybe use advanced data structure?

    public VNFSenderThread(int instanceID, int ccStrategy, int statePartitionStart, int statePartitionEnd, int stateRange,
                           String csvFilePath, CyclicBarrier instancesBarrier, int expectedRequestCount) {
        this.instanceID = instanceID;
        this.ccStrategy = ccStrategy;
        this.statePartitionStart = statePartitionStart;
        this.statePartitionEnd = statePartitionEnd;
        this.stateRange = stateRange;
        this.csvFilePath = csvFilePath;
        this.instancesBarrier = instancesBarrier;
        this.expectedRequestCount = expectedRequestCount;
        for (int i = 0; i <= stateRange; i++) {
            localStateMap.put(i, stateDefaultValue);
        }
    }

    public void submitFinishedRequest(VNFRequest request) {
        try {
            long requestFinishTime = System.nanoTime();
            latencyList.add(requestFinishTime - request.getCreateTime()); //TODO: Include more information, such as reqID
            finishedRequestQueue.put(request); //TODO: In fact, maintaining such queue is not necessary, recording latency is enough?
            finishedRequestCount++;

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

                if (ccStrategy == 0) { // local state access
                    long timestamp = System.nanoTime();
                    if (tupleID >= statePartitionStart && tupleID <= statePartitionEnd) {
                        executeUDF(tupleID, type, 0);
                        finishedRequestQueue.add(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));
//                        VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));

                    } else { // cross-partition state access
                        PartitionCCThread.submitPartitionRequest(new PartitionData(timestamp, reqID, instanceID, tupleID, 0, -1, managerResponseQueue));
                        int response = managerResponseQueue.take(); // Wait for response from StateManager
                        if (response == 1) {
//                            VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));
                            finishedRequestQueue.add(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));
                        } else {
                            System.err.println("Error in processing cross-partition request to tuple: " + tupleID);
                        }
                    }

                } else if (ccStrategy == 1) { // Replication
                    long timestamp = System.nanoTime();
                    if (type == 0) { // read
                        if (!managerSyncQueue.isEmpty()) {
                            processAllQueueItems();
                        }
                        executeUDF(tupleID, type, 0);
                        finishedRequestQueue.add(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));
//                        VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));

                    } else if (type == 1 || type == 2) { // write
                        executeUDF(tupleID, type, 0);
                        CacheCCThread.submitReplicationRequest(new CacheData(timestamp, tupleID, instanceID, 0, managerResponseQueue));
                        int response = managerResponseQueue.take(); // Wait for response from StateManager
                        if (response == 1) {
                            finishedRequestQueue.add(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));
//                            VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));
                        } else {
                            System.err.println("Error in sync updates to instance caches: " + tupleID);
                        }
                    }

                } else if (ccStrategy == 2) { // Offload
                    long timestamp = System.nanoTime();
                    OffloadCCThread.submitOffloadReq(new OffloadData(timestamp, instanceID, reqID, tupleID, 0, 0, 0, type, managerResponseQueue));
                    if (type != 1) {
                        int response = managerResponseQueue.take(); // Wait for response from StateManager, blocking
                    }

                } else if (ccStrategy == 3) { // TPG
                    long timestamp = System.nanoTime();
                    tpgQueues.get(tpgRequestCount % numSpouts).offer(new TransactionalVNFEvent(type, instanceID, timestamp, reqID, tupleID, 0, 0, 0));
                    tpgRequestCount++;
                }
            }

            System.out.println("Instance " + instanceID + " initiated " + inputLineCounter + " requests.");
            assert inputLineCounter == expectedRequestCount;

            while (finishedRequestCount < expectedRequestCount) {
                Thread.sleep(1000);
            }

            int arrivedIndex = instancesBarrier.await(); // Wait for other instances
            if (arrivedIndex == 0) {
                System.out.println("All instances have finished, sending stop signals to StateManager...");
                MonitorThread.submitPatternData(new PatternData(-1, instanceID, 0, false));
                PartitionCCThread.submitPartitionRequest(new PartitionData(-1, 0, instanceID, 0, 0, -1));
                CacheCCThread.submitReplicationRequest(new CacheData(-1, 0, instanceID, 0));
                OffloadCCThread.submitOffloadReq(new OffloadData(-1, instanceID, 0, 0, 0, 0, 0, 0));
                for (int tpgQueueIndex = 0; tpgQueueIndex < tpgQueues.size(); tpgQueueIndex++) {
                    tpgQueues.get(tpgQueueIndex).offer(new TransactionalVNFEvent(0, instanceID, -1, 0, 0, 0, 0, 0));
                }

                // Compute instance local performance
                overallEndTime = latencyList.get(latencyList.size() - 1);
                long overallDuration = overallEndTime - overallStartTime;
                System.out.println("VNF receiver " + instanceID + " processed all " + finishedRequestCount + " requests with throughput " + (finishedRequestCount / (overallDuration / 1E9)) + " events/second");

                // Notify performance computation
                MorphStreamEnv.get().simVNFLatch.countDown();
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
        return finishedRequestCount;
    }
    public long getOverallStartTime() {
        return overallStartTime;
    }
    public long getOverallEndTime() {
        return overallEndTime;
    }
    public ArrayList<Long> getLatencyList() {
        return latencyList;
    }

    //TODO: Implement Lock for reading and writing states
    public int readLocalState(int tupleID) {
        return localStateMap.get(tupleID);
    }
    public void writeLocalState(int tupleID, int value) {
        localStateMap.put(tupleID, value);
    }
}
