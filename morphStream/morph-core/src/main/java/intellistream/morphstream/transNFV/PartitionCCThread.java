package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionCCThread implements Runnable {
    private static BlockingQueue<VNFRequest> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance.
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private static long managerEventSyncTime = 0;
    private static long managerEventUsefulTime = 0;

    public PartitionCCThread(BlockingQueue<VNFRequest> operationQueue, HashMap<Integer, Integer> partitionOwnership) {
        PartitionCCThread.operationQueue = operationQueue;
        PartitionCCThread.partitionOwnership = partitionOwnership;
        instanceSocketMap = MorphStreamEnv.ourInstance.instanceSocketMap();
    }

    public static void submitPartitionRequest(VNFRequest vnfRequest) {
        try {
            operationQueue.put(vnfRequest);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {

        if (communicationChoice == 0) {
            while (!Thread.currentThread().isInterrupted()) {
                VNFRequest request;
                try {
                    request = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (request.getCreateTime() == -1) {
                    System.out.println("Partition CC thread received stop signal");
                    break;
                }
                int targetInstanceID = partitionOwnership.get(request.getTupleID());

                // Simulating cross-partition state access
                try {
                    long syncStartTime = System.nanoTime();
                    int targetPartitionState = VNFRunner.getSender(targetInstanceID).readLocalState(request.getTupleID());
                    managerEventSyncTime += System.nanoTime() - syncStartTime;

                    long usefulStartTime = System.nanoTime();
                    VNFManagerUDF.executeUDF(request);
                    managerEventUsefulTime += System.nanoTime() - usefulStartTime; //TODO: This is not accurate, the actual state access is performed at target instance

                    long syncStartTime2 = System.nanoTime();
                    VNFRunner.getSender(targetInstanceID).writeLocalState(request.getTupleID(), targetPartitionState);
                    managerEventSyncTime += System.nanoTime() - syncStartTime2;

                    VNFRunner.getSender(request.getInstanceID()).submitFinishedRequest(request);

                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                }
            }

        } else if (communicationChoice == 1) {
            throw new UnsupportedOperationException();
        }
    }

    public static long getManagerEventSyncTime() {
        return managerEventSyncTime;
    }

    public static long getManagerEventUsefulTime() {
        return managerEventUsefulTime;
    }
}
