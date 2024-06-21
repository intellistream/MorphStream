package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.transNFV.data.SyncData;
import intellistream.morphstream.transNFV.simVNF.VNFInstance;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ReplicationCCThread implements Runnable {
    private static BlockingQueue<VNFRequest> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static long initEndTime = -1;
    private static long processEndTime = -1;

    public ReplicationCCThread(BlockingQueue<VNFRequest> operationQueue) {
        ReplicationCCThread.operationQueue = operationQueue;
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
                for (Map.Entry<Integer, VNFInstance> entry : VNFRunner.getSenderMap().entrySet()) {
                    if (entry.getKey() != request.getInstanceID()) {
                        SyncData syncData = new SyncData(request.getCreateTime(), request.getTupleID(), request.getValue());
                        entry.getValue().submitSyncData(syncData);
                    }
                }

                try {
                    VNFRunner.getSender(request.getInstanceID()).submitFinishedRequest(request);
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
