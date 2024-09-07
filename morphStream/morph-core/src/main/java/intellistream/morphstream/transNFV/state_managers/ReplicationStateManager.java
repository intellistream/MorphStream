package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.transNFV.common.SyncData;
import intellistream.morphstream.transNFV.vnf.VNFInstance;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ReplicationStateManager implements Runnable {
    private static BlockingQueue<VNFRequest> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static long initEndTime = -1;
    private static long processEndTime = -1;

    public ReplicationStateManager(BlockingQueue<VNFRequest> operationQueue) {
        ReplicationStateManager.operationQueue = operationQueue;
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    }

    public static void submitReplicationRequest(VNFRequest request) {
        try {
            operationQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        initEndTime = System.nanoTime();

        if (communicationChoice == 0) { // Java sim VNF
            while (!Thread.currentThread().isInterrupted()) {
                VNFRequest request;
                try {
                    request = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (request.getCreateTime() == -1) {
                    processEndTime = System.nanoTime();
                    writeCSVTimestamps();
                    System.out.println("Cache CC thread received stop signal");
                    break;
                }

                // Simulating state update synchronization to other instances
                for (Map.Entry<Integer, VNFInstance> entry : VNFManager.getAllInstances().entrySet()) {
                    if (entry.getKey() != request.getInstanceID()) {
                        SyncData syncData = new SyncData(request.getTupleID(), request.getValue());
                        entry.getValue().addStateSync(syncData);
                    }
                }

                try {
                    VNFManager.getInstance(request.getInstanceID()).submitFinishedRequest(request);
                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                }

            }

        } else if (communicationChoice == 1) {
            throw new RuntimeException("Not implemented");
        }
    }
    private static void writeCSVTimestamps() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "timestamps");
        String filePath = String.format("%s/%s.csv", baseDirectory, "Replication");
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

}
