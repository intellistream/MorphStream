package intellistream.morphstream.api.input;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CHCController implements Runnable {
    private static BlockingQueue<OffloadData> requestQueue; // Assume all states are sharing by all instances
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private static final ConcurrentHashMap<Integer, Integer> tupleOwnership = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> fetchedValues = MorphStreamEnv.fetchedValues;
    private static long aggSyncTime = 0;
    private static long aggUsefulTime = 0;

    public CHCController(BlockingQueue<OffloadData> requestQueue) {
        CHCController.requestQueue = requestQueue;
    }

    public static void submitCHCReq(OffloadData request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        if (communicationChoice == 1) {
            throw new RuntimeException("Remote communication not supported");

        } else if (communicationChoice == 0) {
            System.out.println("Flushing Controller has started.");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    OffloadData request;
                    request = requestQueue.take();
                    if (request.getTimeStamp() == -1) {
                        System.out.println("Flushing CC thread received stop signal");
                        break;
                    }
                    int instanceID = request.getInstanceID();
                    int tupleID = request.getTupleID();
                    long timeStamp = request.getTimeStamp();
                    long txnReqId = request.getTxnReqId();

                    if (tupleOwnership.get(tupleID) == null) { // State ownership is not yet assigned, assign it to the current instance
                        long syncStartTime = System.nanoTime();
                        tupleOwnership.put(tupleID, instanceID);
                        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                        VNFRunner.getSender(instanceID).writeLocalState(tupleID, readRecord.getValues().get(1).getInt());
                        aggSyncTime += System.nanoTime() - syncStartTime;

                    } else if (tupleOwnership.get(tupleID) == instanceID) { // State ownership is still the same, allow instance to perform local RW
                        //TODO: Simulate permission for instance to do local state access
//                        VNFRunner.getSender(instanceID).permitLocalStateAccess(tupleID);

                    } else { // State ownership has changed, fetch state from the current owner and perform RW centrally
                        int currentOwner = tupleOwnership.get(tupleID);
                        long syncStartTime = System.nanoTime();
                        int tupleValue = VNFRunner.getSender(currentOwner).readLocalState(tupleID);
                        aggSyncTime += System.nanoTime() - syncStartTime;

                        long usefulStartTime = System.nanoTime();
                        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                        SchemaRecord tempo_record = new SchemaRecord(readRecord);
                        simUDF(tupleValue);
                        tempo_record.getValues().get(1).setInt(tupleValue);
                        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
                        aggUsefulTime += System.nanoTime() - usefulStartTime;
                    }

                    VNFRequest response = new VNFRequest((int) txnReqId, instanceID, tupleID, 0, timeStamp);
                    VNFRunner.getSender(instanceID).submitFinishedRequest(response);

                } catch (InterruptedException | DatabaseException | RuntimeException e) {
                    System.out.println("CHC Interrupted exception: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }

        } else {
            throw new RuntimeException("Invalid communication choice");
        }
    }

    private int simUDF(int tupleValue) {
//        Thread.sleep(10);
        //TODO: Simulate UDF better
        return tupleValue;
    }

    public static long getAggSyncTime() {
        return aggSyncTime;
    }

    public static long getAggUsefulTime() {
        return aggUsefulTime;
    }

}
