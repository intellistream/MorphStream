package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;
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

public class OpenNFController implements Runnable {
    // TODO: Add partitionable-state performance study later, each state-partition should be handled by one queue
    private static BlockingQueue<VNFRequest> requestQueue; // Assume all states are sharing by all instances
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private static long aggUsefulTime = 0;
    private static long initEndTime = -1;
    private static long processEndTime = -1;

    public OpenNFController(BlockingQueue<VNFRequest> requestQueue) {
        OpenNFController.requestQueue = requestQueue;
    }

    public static void submitOpenNFReq(VNFRequest request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        initEndTime = System.nanoTime();

        if (communicationChoice == 1) {
            throw new RuntimeException("Remote communication not supported");

        } else if (communicationChoice == 0) {
            System.out.println("Broadcasting Controller started.");
            while (!Thread.currentThread().isInterrupted()) {
                VNFRequest request;

                try {
                    request = requestQueue.take();
                    if (request.getCreateTime() == -1) {
                        processEndTime = System.nanoTime();
                        writeCSVTimestamps();
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
                    VNFManagerUDF.executeUDF(request);
                    aggUsefulTime += System.nanoTime() - usefulStartTime;

                    SchemaRecord tempo_record = new SchemaRecord(readRecord);
                    tempo_record.getValues().get(1).setInt(readValue);
                    tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

                    for (int i = 0; i < vnfInstanceNum; i++) {
                        VNFRunner.getSender(i).writeLocalState(tupleID, readValue);
                    }

                    //TODO: Here we should add lock to all instance, but since OpenNF only has a single controller thread, it is fine
                    VNFRunner.getSender(instanceID).submitFinishedRequest(request);

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (DatabaseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void writeCSVTimestamps() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
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

    public static long getAggUsefulTime() {
        return aggUsefulTime;
    }
}



