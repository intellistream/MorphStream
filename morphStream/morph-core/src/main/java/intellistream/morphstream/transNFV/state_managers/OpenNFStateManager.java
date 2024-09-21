package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.transNFV.vnf.UDF;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.*;

public class OpenNFStateManager implements Runnable {
    // TODO: Add partitionable-state performance study later, each state-partition should be handled by one queue
    private BlockingQueue<VNFRequest> requestQueue; // Assume all states are sharing by all instances
    private final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private long aggUsefulTime = 0;
    private long initEndTime = -1;
    private long processEndTime = -1;

    public OpenNFStateManager(BlockingQueue<VNFRequest> requestQueue) {
        this.requestQueue = requestQueue;
    }

    public void submitOpenNFReq(VNFRequest request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        initEndTime = System.nanoTime();

        System.out.println("Broadcasting Controller started.");
        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;

            try {
                request = requestQueue.take();
                if (request.getCreateTime() == -1) {
                    processEndTime = System.nanoTime();
//                    writeCSVTimestamps();
                    System.out.println("Broadcasting Controller received stop signal");
                    break;
                }
                int instanceID = request.getInstanceID();
                int tupleID = request.getTupleID();
                long timeStamp = request.getCreateTime();
                long txnReqId = request.getReqID();


                TableRecord tableRecord;
                tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                int readValue = readRecord.getValues().get(1).getInt();

                long usefulStartTime = System.nanoTime();
                UDF.executeUDF(request);
                aggUsefulTime += System.nanoTime() - usefulStartTime;

                SchemaRecord tempo_record = new SchemaRecord(readRecord);
                tempo_record.getValues().get(1).setInt(readValue);
                tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

                for (int i = 0; i < numInstances; i++) {
                    VNFManager.getInstanceStateManager(i).nullSafeStateUpdate(tupleID, readValue);
                }

                //TODO: Here we should add lock to all instance, but since OpenNF only has a single controller thread, it is fine
                VNFManager.getInstance(instanceID).submitFinishedRequest(request);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeCSVTimestamps() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "timestamps");
        String filePath = String.format("%s/%s.csv", baseDirectory, "OpenNF");
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

    public long getAggUsefulTime() {
        return aggUsefulTime;
    }
}



