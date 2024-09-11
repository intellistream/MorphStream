package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.common.SyncData;
import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.transNFV.vnf.VNFInstance;
import intellistream.morphstream.transNFV.vnf.VNFManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class PartitionStateManager implements Runnable {
    private static BlockingQueue<VNFRequest> operationQueue;
    private static long initEndTime = -1;
    private static long processEndTime = -1;
    private final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private final int partitionRange = NUM_ITEMS / numInstances;

    public PartitionStateManager(BlockingQueue<VNFRequest> operationQueue) {
        PartitionStateManager.operationQueue = operationQueue;
    }

    public static void submitPartitioningRequest(VNFRequest request) {
        try {
            operationQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        initEndTime = System.nanoTime();

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
                System.out.println("Partitioning CC thread received stop signal");
                break;
            }

            int targetInstanceID = getPartitionInstance(request.getTupleID());
            VNFInstance targetInstance = VNFManager.getInstance(targetInstanceID);
            targetInstance.getLocalSVCCStateManager().executeTransaction(request);

            try {
                VNFManager.getInstance(request.getInstanceID()).submitFinishedRequest(request);
            } catch (NullPointerException e) {
                throw new RuntimeException(e);
            }

        }
    }

    private int getPartitionInstance(int tupleID) {
        return tupleID / partitionRange;
    }


    private static void writeCSVTimestamps() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
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

}
