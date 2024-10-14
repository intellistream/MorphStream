package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.transNFV.vnf.VNFInstance;
import intellistream.morphstream.transNFV.vnf.VNFManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class PartitionStateManager implements Runnable {
    private BlockingQueue<VNFRequest> operationQueue;
    private long initEndTime = -1;
    private long processEndTime = -1;
    private final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private final int partitionRange = NUM_ITEMS / numInstances;

    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private long usefulStartTime = 0;
    private long parsingStartTime = 0;
    private long AGG_USEFUL_TIME = 0;
    private long AGG_PARSING_TIME = 0;


    public PartitionStateManager(BlockingQueue<VNFRequest> operationQueue) {
        this.operationQueue = operationQueue;
    }

    public void submitPartitioningRequest(VNFRequest request) {
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
//                writeCSVTimestamps();
                System.out.println("Partitioning CC thread received stop signal");
                break;
            }

            // Cross-partition transaction execution
            REC_usefulStartTime();
            int targetInstanceID = getPartitionInstance(request.getTupleID());
            VNFInstance targetInstance = VNFManager.getInstance(targetInstanceID);
            targetInstance.getLocalSVCCStateManager().nonBlockingTxnExecution(request);
            REC_usefulEndTime();

            REC_parsingStartTime();
            try {
                VNFManager.getInstance(request.getInstanceID()).submitACK(request);
            } catch (NullPointerException e) {
                throw new RuntimeException(e);
            }
            REC_parsingEndTime();

        }
    }

    private int getPartitionInstance(int tupleID) {
        return tupleID / partitionRange;
    }

    private void REC_usefulStartTime() {
        if (enableTimeBreakdown) {
            usefulStartTime = System.nanoTime();
        }
    }

    private void REC_usefulEndTime() {
        if (enableTimeBreakdown) {
            AGG_USEFUL_TIME += System.nanoTime() - usefulStartTime;
        }
    }

    private void REC_parsingStartTime() {
        if (enableTimeBreakdown) {
            parsingStartTime = System.nanoTime();
        }
    }

    private void REC_parsingEndTime() {
        if (enableTimeBreakdown) {
            AGG_PARSING_TIME += System.nanoTime() - parsingStartTime;
        }
    }

    public long getAGG_USEFUL_TIME() {
        return AGG_USEFUL_TIME;
    }

    public long getAGG_PARSING_TIME() {
        return AGG_PARSING_TIME;
    }

    private void writeCSVTimestamps() {
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
