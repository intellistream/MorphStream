package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.transNFV.vnf.UDF;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionStateManager implements Runnable {
    private static BlockingQueue<VNFRequest> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance.
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private static long managerEventSyncTime = 0;
    private static long managerEventUsefulTime = 0;
    private static long initEndTime = -1;
    private static long processEndTime = -1;

    public PartitionStateManager(BlockingQueue<VNFRequest> operationQueue, HashMap<Integer, Integer> partitionOwnership) {
        PartitionStateManager.operationQueue = operationQueue;
        PartitionStateManager.partitionOwnership = partitionOwnership;
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
        initEndTime = System.nanoTime();

        if (communicationChoice == 0) {
            while (!Thread.currentThread().isInterrupted()) {
                VNFRequest request;
                try {
                    request = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (request.getCreateTime() == -1) {
                    processEndTime = System.nanoTime();
                    System.out.println("Partition CC thread received stop signal");
                    writeCSVTimestamps();
                    break;
                }

                int targetInstanceID = partitionOwnership.get(request.getTupleID());

                // Simulating cross-partition state access
                try {
                    long syncStartTime = System.nanoTime();
                    int targetPartitionState = VNFManager.getSender(targetInstanceID).readLocalState(request.getTupleID());
                    managerEventSyncTime += System.nanoTime() - syncStartTime;

                    long usefulStartTime = System.nanoTime();
                    UDF.executeUDF(request);
                    managerEventUsefulTime += System.nanoTime() - usefulStartTime; //TODO: This is not accurate, the actual state access is performed at target instance

                    long syncStartTime2 = System.nanoTime();
                    VNFManager.getSender(targetInstanceID).writeLocalState(request.getTupleID(), targetPartitionState);
                    managerEventSyncTime += System.nanoTime() - syncStartTime2;

                    VNFManager.getSender(request.getInstanceID()).submitFinishedRequest(request);

                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                }
            }

        } else if (communicationChoice == 1) {
            throw new UnsupportedOperationException();
        }
    }

    private static void writeCSVTimestamps() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "timestamps");
        String filePath = String.format("%s/%s.csv", baseDirectory, "Partitioning");
        System.out.println("Writing to " + filePath);
        File dir = new File(baseDirectory);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return;
            }
        }
        File file = new File(filePath);
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                System.out.println("Failed to delete existing file.");
                return;
            }
        }
        try (FileWriter fileWriter = new FileWriter(file)) {
            String lineToWrite = initEndTime + "," + processEndTime + "\n";
            fileWriter.write(lineToWrite);
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
        }
    }

    public static long getManagerEventSyncTime() {
        return managerEventSyncTime;
    }
    public static long getManagerEventUsefulTime() {
        return managerEventUsefulTime;
    }
}
