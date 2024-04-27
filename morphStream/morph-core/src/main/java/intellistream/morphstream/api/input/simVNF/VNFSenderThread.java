package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class VNFSenderThread implements Runnable {
//Four parallel instances, each maintaining:
//A static CC option
//A range of state partition
//A local hashmap for states
//Two threads (sender and receiver)
//A FIFO queue of completed requests from the state manager
//Some APIs for reading and updating the hashmaps
//A hardcoded VNF function.

//Sender thread
//Read string from CSV file, create request object (reqID, instanceID, tupleID, type)
//Record system time into request object.
//For remote request: forward request to the corresponding CC manager
//For local request: execute local VNF function, forward request to receiver thread

//Receiver thread
//Offer a submitRequest() method for sender / manager to add finished requests
//Take one finished request from the queue at a time
//Record system time into request object
//Finally use a performance calculator tool to compute overall throughput and latency

    private int instanceID;
    private String csvFilePath;
    private int ccStrategy;
    private int stateStartID; //local partition starting tupleID
    private int stateEndID; //local partition ending tupleID
    private int stateRange; //entire state space
    private int stateDefaultValue = 0;
    private HashMap<Integer, Integer> localStateMap = new HashMap<>();
    private ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = AdaptiveCCManager.tpgQueues;
    private int requestCounter = 0;
    private int numSpouts = 4;
    private int lineCounter = 0;

    public VNFSenderThread(int instanceID, int ccStrategy, int stateStartID, int stateEndID, int stateRange, String csvFilePath) {
        this.instanceID = instanceID;
        this.ccStrategy = ccStrategy;
        this.stateStartID = stateStartID;
        this.stateEndID = stateEndID;
        this.stateRange = stateRange;
        this.csvFilePath = csvFilePath;
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
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                int reqID = Integer.parseInt(parts[0]);
                int tupleID = Integer.parseInt(parts[1]);
                int type = Integer.parseInt(parts[2]);
                VNFRequest request = new VNFRequest(reqID, instanceID, tupleID, type, System.currentTimeMillis());

                lineCounter++;

                if (ccStrategy == 0) { // Partition
                    if (tupleID >= stateStartID && tupleID <= stateEndID) {
                        vnfFunction(tupleID, type, 0);
                        VNFThreadManager.getReceiver(instanceID).submitFinishedRequest(request);

                    } else {
                        PartitionCCThread.submitPartitionRequest(new PartitionData(tupleID, instanceID, 0));
                    }

                } else if (ccStrategy == 1) { // Replication
                    if (type == 0) { // read
                        vnfFunction(tupleID, type, 0);
                        VNFThreadManager.getReceiver(instanceID).submitFinishedRequest(request);

                    } else if (type == 1) { // write
                        CacheCCThread.submitReplicationRequest(new CacheData(tupleID, instanceID, 0));
                    }

                } else if (ccStrategy == 2) { // Offload
                    OffloadCCThread.submitOffloadReq(new OffloadData(instanceID, System.currentTimeMillis(), reqID, tupleID, 0, 0, 0));

                } else if (ccStrategy == 3) { // TPG
                    tpgQueues.get(requestCounter % numSpouts).offer(new TransactionalVNFEvent(instanceID, System.currentTimeMillis(), reqID, tupleID, 0, 0, 0));
                    requestCounter++;
                }

            }
            System.out.println("Instance " + instanceID + " processed " + lineCounter + " requests.");
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

    public int readLocalState(int tupleID) {
        return localStateMap.get(tupleID);
    }

    public void updateLocalState(int tupleID, int value) {
        localStateMap.put(tupleID, value);
    }
}
