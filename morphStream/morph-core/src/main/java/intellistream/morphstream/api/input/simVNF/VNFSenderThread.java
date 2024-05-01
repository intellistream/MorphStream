package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class VNFSenderThread implements Runnable {
    private int instanceID;
    private String csvFilePath;
    private int ccStrategy;
    private int statePartitionStart;
    private int statePartitionEnd;
    private int stateRange; //entire state space
    private int stateDefaultValue = 0;
    private HashMap<Integer, Integer> localStateMap = new HashMap<>();
    private ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = AdaptiveCCManager.tpgQueues;
    private final int numSpouts = MorphStreamEnv.get().configuration().getInt("tthread");
    private int requestCounter = 0;
    private int lineCounter = 0;
    private long startTime;

    public VNFSenderThread(int instanceID, int ccStrategy, int statePartitionStart, int statePartitionEnd, int stateRange, String csvFilePath) {
        this.instanceID = instanceID;
        this.ccStrategy = ccStrategy;
        this.statePartitionStart = statePartitionStart;
        this.statePartitionEnd = statePartitionEnd;
        this.stateRange = stateRange;
        this.csvFilePath = csvFilePath;
        for (int i = 0; i <= stateRange; i++) {
            localStateMap.put(i, stateDefaultValue);
        }
    }

    @Override
    public void run() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("VNF sender instance " + instanceID + " started.");
        startTime = System.nanoTime();

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(csvFilePath));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                int reqID = Integer.parseInt(parts[0]);
                int tupleID = Integer.parseInt(parts[1]);
                int type = Integer.parseInt(parts[2]);

                lineCounter++;

                if (ccStrategy == 0) { // Partition
                    if (tupleID >= statePartitionStart && tupleID <= statePartitionEnd) {
                        vnfFunction(tupleID, type, 0);
                        VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, System.currentTimeMillis()));

                    } else { //Let partition manager handle the request
                        PartitionCCThread.submitPartitionRequest(new PartitionData(System.currentTimeMillis(), reqID, instanceID, tupleID, 0));
                    }

                } else if (ccStrategy == 1) { // Replication
                    if (type == 0) { // read
                        vnfFunction(tupleID, type, 0);
                        //TODO: Consider adding a delay here to simulate synchronization check
                        VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, System.currentTimeMillis()));

                    } else if (type == 1) { // write
                        vnfFunction(tupleID, type, 0);
                        CacheCCThread.submitReplicationRequest(new CacheData(0, tupleID, instanceID, 0));
                        VNFManager.getReceiver(instanceID).submitFinishedRequest(new VNFRequest(reqID, instanceID, tupleID, type, System.currentTimeMillis()));
                    }

                } else if (ccStrategy == 2) { // Offload
                    OffloadCCThread.submitOffloadReq(new OffloadData(System.currentTimeMillis(), instanceID, reqID, tupleID, 0, 0, 0, type));

                } else if (ccStrategy == 3) { // TPG
                    tpgQueues.get(requestCounter % numSpouts).offer(new TransactionalVNFEvent(type, instanceID, System.currentTimeMillis(), reqID, tupleID, 0, 0, 0));
                    requestCounter++;
                }

            }
            System.out.println("Sender instance " + instanceID + " processed " + lineCounter + " requests.");
        } catch (IOException e) {
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

    public long getStartTime() {
        return startTime;
    }

    public int readLocalState(int tupleID) {
        return localStateMap.get(tupleID);
    }

    public void writeLocalState(int tupleID, int value) {
        localStateMap.put(tupleID, value);
    }
}
