package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.*;

public class VNFSenderThread implements Runnable {
    private int instanceID;
    private String csvFilePath;
    private int ccStrategy;
    private int statePartitionStart;
    private int statePartitionEnd;
    private int stateRange; //entire state space
    private int stateDefaultValue = 0;
    private final CyclicBarrier finishBarrier;
    private ConcurrentHashMap<Integer, Integer> localStateMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = AdaptiveCCManager.tpgQueues;
    private BlockingQueue<SyncData> managerSyncQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<Integer> managerResponseQueue = new LinkedBlockingQueue<>();
    private final int numSpouts = MorphStreamEnv.get().configuration().getInt("tthread");
    private int requestCounter = 0;
    private int lineCounter = 0;
    private long overallStartTime;

    public VNFSenderThread(int instanceID, int ccStrategy, int statePartitionStart, int statePartitionEnd, int stateRange, String csvFilePath, CyclicBarrier finishBarrier) {
        this.instanceID = instanceID;
        this.ccStrategy = ccStrategy;
        this.statePartitionStart = statePartitionStart;
        this.statePartitionEnd = statePartitionEnd;
        this.stateRange = stateRange;
        this.csvFilePath = csvFilePath;
        this.finishBarrier = finishBarrier;
        for (int i = 0; i <= stateRange; i++) {
            localStateMap.put(i, stateDefaultValue);
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

                lineCounter++;

                if (ccStrategy == 0) { // local state access
                    long timestamp = System.nanoTime();
                    if (tupleID >= statePartitionStart && tupleID <= statePartitionEnd) {
                        vnfFunction(tupleID, type, 0);
                        VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));

                    } else { // cross-partition state access
                        PartitionCCThread.submitPartitionRequest(new PartitionData(timestamp, reqID, instanceID, tupleID, 0, -1, managerResponseQueue));
                        int response = managerResponseQueue.take(); // Wait for response from StateManager
                        if (response == 1) {
                            VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));
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
                        vnfFunction(tupleID, type, 0);
                        VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));

                    } else if (type == 1 || type == 2) { // write
                        vnfFunction(tupleID, type, 0);
                        CacheCCThread.submitReplicationRequest(new CacheData(timestamp, tupleID, instanceID, 0, managerResponseQueue));
                        int response = managerResponseQueue.take(); // Wait for response from StateManager
                        if (response == 1) {
                            VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, timestamp));
                        } else {
                            System.err.println("Error in sync updates to instance caches: " + tupleID);
                        }
                    }

                } else if (ccStrategy == 2) { // Offload
                    long timestamp = System.nanoTime();
                    OffloadCCThread.submitOffloadReq(new OffloadData(timestamp, instanceID, reqID, tupleID, 0, 0, 0, type, managerResponseQueue));
                    if (type != 1) {
                        int response = managerResponseQueue.take(); // Wait for response from StateManager
                    }

                } else if (ccStrategy == 3) { // TPG
                    long timestamp = System.nanoTime();
                    tpgQueues.get(requestCounter % numSpouts).offer(new TransactionalVNFEvent(type, instanceID, timestamp, reqID, tupleID, 0, 0, 0));
                    requestCounter++;
                }
            }

            System.out.println("Sender instance " + instanceID + " processed " + lineCounter + " requests.");
            Thread.sleep(1000);
            int arrivedIndex = finishBarrier.await();

            if (arrivedIndex == 0) { // Notify all receivers for the end of workload
                int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
                for (int i=0; i<vnfInstanceNum; i++) {
                    VNFManager.getReceiver(i).submitFinishedRequest(new VNFRequest(0, instanceID, 0, 0, -1));
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

    private int vnfFunction(int tupleID, int type, int value) {
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

    public long getOverallStartTime() {
        return overallStartTime;
    }

    public int readLocalState(int tupleID) {
        return localStateMap.get(tupleID);
    }

    public void writeLocalState(int tupleID, int value) {
        localStateMap.put(tupleID, value);
    }
}
