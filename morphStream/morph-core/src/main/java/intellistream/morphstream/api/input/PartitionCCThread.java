package intellistream.morphstream.api.input;

import intellistream.morphstream.api.input.simVNF.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PartitionCCThread implements Runnable {
    private static BlockingQueue<PartitionData> operationQueue;
    private static ConcurrentHashMap<Integer, Integer> fetchedValues = new ConcurrentHashMap<>(); // tupleID (fetching from another instance) -> value
    private final Map<Integer, Socket> instanceSocketMap;
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance.
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);
    //TODO: The key should labels partition start index as optimization

    public PartitionCCThread(BlockingQueue<PartitionData> operationQueue, HashMap<Integer, Integer> partitionOwnership) {
        PartitionCCThread.operationQueue = operationQueue;
        PartitionCCThread.partitionOwnership = partitionOwnership;
        instanceSocketMap = MorphStreamEnv.ourInstance.instanceSocketMap();
    }

    public static void submitPartitionRequest(PartitionData partitionData) {
        try {
            operationQueue.put(partitionData);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {

        if (serveRemoteVNF) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    PartitionData partitionData = operationQueue.take();
                    if (partitionData.getTimeStamp() == -1) {
                        System.out.println("Partition CC thread received stop signal");
                        break;
                    }

                    int targetInstanceID = partitionOwnership.get(partitionData.getTupleID());
                    int tupleID = partitionData.getTupleID();

                    AdaptiveCCManager.vnfStubs.get(targetInstanceID).fetch_value(tupleID); //Send fetch_value request to target instance
                    // Wait until the value is fetched with a timeout
                    while (!fetchedValues.containsKey(tupleID)) {
                        try {
                            // Avoid busy-waiting
                            TimeUnit.MILLISECONDS.sleep(50); //TODO: Make sure it does not introduce bottleneck
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                    fetchedValues.remove(tupleID);
                    AdaptiveCCManager.vnfStubs.get(partitionData.getInstanceID()).txn_handle_done(partitionData.getTxnReqId());

                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            }

        } else {
            while (!Thread.currentThread().isInterrupted()) {
                PartitionData partitionData;
                try {
                    partitionData = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (partitionData.getTimeStamp() == -1) {
                    System.out.println("Partition CC thread received stop signal");
                    break;
                }
                int targetInstanceID = partitionOwnership.get(partitionData.getTupleID());

                // Simulating cross-partition state access
                try {
                    int targetPartitionState = VNFManager.getSender(targetInstanceID).readLocalState(partitionData.getTupleID());
                    VNFManager.getSender(targetInstanceID).writeLocalState(partitionData.getTupleID(), targetPartitionState);
                    //TODO: Add locking here
                    partitionData.getSenderResponseQueue().add(1);

                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
