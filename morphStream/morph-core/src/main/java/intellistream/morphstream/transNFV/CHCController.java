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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class CHCController implements Runnable {
    private static BlockingQueue<VNFRequest> requestQueue; // Assume all states are sharing by all instances
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private static final ConcurrentHashMap<Integer, Integer> tupleOwnership = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> fetchedValues = MorphStreamEnv.fetchedValues;
    private static long aggUsefulTime = 0;
    private static long initEndTime = -1;
    private static long processEndTime = -1;

    public CHCController(BlockingQueue<VNFRequest> requestQueue) {
        CHCController.requestQueue = requestQueue;
    }

    public static void submitCHCReq(VNFRequest request) {
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
            System.out.println("Flushing Controller has started.");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    VNFRequest request;
                    request = requestQueue.take();
                    if (request.getCreateTime() == -1) {
                        processEndTime = System.nanoTime();
                        writeCSVTimestamps();
                        System.out.println("Flushing CC thread received stop signal");
                        break;
                    }
                    int instanceID = request.getInstanceID();
                    int tupleID = request.getTupleID();
                    long timeStamp = request.getCreateTime();
                    long txnReqId = request.getReqID();

                    if (tupleOwnership.get(tupleID) == null) { // State ownership is not yet assigned, assign it to the current instance
                        long syncStartTime = System.nanoTime();
                        tupleOwnership.put(tupleID, instanceID);
                        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                        VNFRunner.getSender(instanceID).writeLocalState(tupleID, readRecord.getValues().get(1).getInt());

                    } else if (tupleOwnership.get(tupleID) == instanceID) { // State ownership is still the same, allow instance to perform local RW
                        //TODO: Simulate permission for instance to do local state access
//                        VNFRunner.getSender(instanceID).permitLocalStateAccess(tupleID);

                    } else { // State ownership has changed, fetch state from the current owner and perform RW centrally
                        int currentOwner = tupleOwnership.get(tupleID);
                        int tupleValue = VNFRunner.getSender(currentOwner).readLocalState(tupleID);

                        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                        SchemaRecord tempo_record = new SchemaRecord(readRecord);

                        long usefulStartTime = System.nanoTime();
                        VNFManagerUDF.executeUDF(request);
                        aggUsefulTime += System.nanoTime() - usefulStartTime;

                        tempo_record.getValues().get(1).setInt(tupleValue);
                        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

                    }

                    VNFRunner.getSender(instanceID).submitFinishedRequest(request);

                } catch (InterruptedException | DatabaseException | RuntimeException e) {
                    System.out.println("CHC Interrupted exception: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }

        } else {
            throw new RuntimeException("Invalid communication choice");
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

    public static long getAggUsefulTime() {
        return aggUsefulTime;
    }

}
