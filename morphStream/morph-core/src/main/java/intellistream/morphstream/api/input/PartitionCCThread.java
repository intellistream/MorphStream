package intellistream.morphstream.api.input;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionCCThread implements Runnable {
    private static BlockingQueue<PartitionData> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance.
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private static long managerEventSyncTime = 0;
    private static long managerEventUsefulTime = 0;

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

        if (communicationChoice == 0) {
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
                    long syncStartTime = System.nanoTime();
                    int targetPartitionState = VNFRunner.getSender(targetInstanceID).readLocalState(partitionData.getTupleID());
                    managerEventSyncTime += System.nanoTime() - syncStartTime;

                    long usefulStartTime = System.nanoTime();
                    simUDF(targetPartitionState); // Simulate UDF
                    managerEventUsefulTime += System.nanoTime() - usefulStartTime; //TODO: This is not accurate, the actual state access is performed at target instance

                    long syncStartTime2 = System.nanoTime();
                    VNFRunner.getSender(targetInstanceID).writeLocalState(partitionData.getTupleID(), targetPartitionState);
                    managerEventSyncTime += System.nanoTime() - syncStartTime2;

                    VNFRequest request = new VNFRequest((int) partitionData.getTxnReqId(), partitionData.getInstanceID(), partitionData.getTupleID(), 0, partitionData.getTimeStamp());
                    VNFRunner.getSender(partitionData.getInstanceID()).submitFinishedRequest(request);

                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                }
            }

        } else if (communicationChoice == 1) {
            while (!Thread.currentThread().isInterrupted()) {
                int srcInstanceID = -1;
                int targetInstanceID = -1;
                try {
                    PartitionData partitionData = operationQueue.take();
                    if (partitionData.getTimeStamp() == -1) {
                        System.out.println("Partition CC thread received stop signal");
                        break;
                    }

                    srcInstanceID = partitionData.getInstanceID();
                    targetInstanceID = partitionOwnership.get(partitionData.getTupleID());
                    int tupleID = partitionData.getTupleID();

                    synchronized (instanceLocks.get(targetInstanceID)) {
                        AdaptiveCCManager.vnfStubs.get(targetInstanceID).fetch_value(tupleID); //TODO: This simulates lock to another partition
//                        AdaptiveCCManager.vnfStubs.get(targetInstanceID).execute_sa_udf(partitionData.getTxnReqId(), partitionData.getSaIndex(), tupleID, 0);
                    }

                    // Wait until the value is fetched with a timeout
                    while (!MorphStreamEnv.fetchedValues.containsKey(tupleID)) {
//                        System.out.println("Partition CC waiting for cross-partition tuple: " + tupleID);
                        Thread.sleep(100);
                    }
                    int cachedValue = MorphStreamEnv.fetchedValues.get(tupleID);
                    System.out.println("Partition CC received cross-partition tuple: " + tupleID);
                    MorphStreamEnv.fetchedValues.remove(tupleID);

                    synchronized (instanceLocks.get(srcInstanceID)) {
                        AdaptiveCCManager.vnfStubs.get(srcInstanceID).txn_handle_done(partitionData.getTxnReqId());
                    }

                } catch (Exception e) {
                    System.out.println("Runtime exception for partition req from instance " + srcInstanceID + " to instance " + targetInstanceID);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private int simUDF(int tupleValue) {
//        Thread.sleep(10);
        //TODO: Simulate UDF better
        return tupleValue;
    }

    public static long getManagerEventSyncTime() {
        return managerEventSyncTime;
    }

    public static long getManagerEventUsefulTime() {
        return managerEventUsefulTime;
    }
}
