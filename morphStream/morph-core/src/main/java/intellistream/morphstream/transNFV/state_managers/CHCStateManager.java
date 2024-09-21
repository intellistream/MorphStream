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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class CHCStateManager implements Runnable {
    private BlockingQueue<VNFRequest> requestQueue; // Assume all states are sharing by all instances
    private final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private final ConcurrentHashMap<Integer, Integer> tupleOwnership = new ConcurrentHashMap<>();
    private long aggUsefulTime = 0;
    private long initEndTime = -1;
    private long processEndTime = -1;

    public CHCStateManager(BlockingQueue<VNFRequest> requestQueue) {
        this.requestQueue = requestQueue;
    }

    public void submitCHCReq(VNFRequest request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        initEndTime = System.nanoTime();

        System.out.println("Flushing Controller has started.");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                VNFRequest request;
                request = requestQueue.take();
                if (request.getCreateTime() == -1) {
                    processEndTime = System.nanoTime();
//                    writeCSVTimestamps();
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
                    VNFManager.getInstanceStateManager(instanceID).nullSafeStateUpdate(tupleID, readRecord.getValues().get(1).getInt());

                } else if (tupleOwnership.get(tupleID) == instanceID) { // State ownership is still the same, allow instance to perform local RW
                    //TODO: Simulate permission for instance to do local state access
//                        VNFRunner.getSender(instanceID).permitLocalStateAccess(tupleID);

                } else { // State ownership has changed, fetch state from the current owner and perform RW centrally
                    int currentOwner = tupleOwnership.get(tupleID);
                    int tupleValue = VNFManager.getInstanceStateManager(instanceID).nullSafeStateRead(tupleID);

                    TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                    SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                    SchemaRecord tempo_record = new SchemaRecord(readRecord);

                    long usefulStartTime = System.nanoTime();
                    UDF.executeUDF(request);
                    aggUsefulTime += System.nanoTime() - usefulStartTime;

                    tempo_record.getValues().get(1).setInt(tupleValue);
                    tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

                }

                VNFManager.getInstance(instanceID).submitFinishedRequest(request);

            } catch (InterruptedException | DatabaseException | RuntimeException e) {
                System.out.println("CHC Interrupted exception: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    private void writeCSVTimestamps() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "timestamps");
        String filePath = String.format("%s/%s.csv", baseDirectory, "CHC");
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
