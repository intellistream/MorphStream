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
import java.util.concurrent.*;

public class OpenNFController implements Runnable {
    // TODO: Add partitionable-state performance study later, each state-partition should be handled by one queue
    private static BlockingQueue<OffloadData> requestQueue; // Assume all states are sharing by all instances
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private static long aggSyncTime = 0;
    private static long aggUsefulTime = 0;

    public OpenNFController(BlockingQueue<OffloadData> requestQueue) {
        OpenNFController.requestQueue = requestQueue;
    }

    public static void submitOpenNFReq(OffloadData request) {
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
            System.out.println("Broadcasting Controller started.");
            while (!Thread.currentThread().isInterrupted()) {
                OffloadData request;
                try {
                    request = requestQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (request.getTimeStamp() == -1) {
                    System.out.println("Broadcasting Controller received stop signal");
                    break;
                }
                int saIndex = request.getSaIndex();
                int instanceID = request.getInstanceID();
                int tupleID = request.getTupleID();
                long timeStamp = request.getTimeStamp();
                long txnReqId = request.getTxnReqId();

                long usefulStartTime = System.nanoTime();
                TableRecord tableRecord;
                try {
                    tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                } catch (DatabaseException e) {
                    throw new RuntimeException(e);
                }
                SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                int readValue = readRecord.getValues().get(1).getInt();

                simUDF(readValue);
                SchemaRecord tempo_record = new SchemaRecord(readRecord);
                tempo_record.getValues().get(1).setInt(readValue);
                tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
                aggUsefulTime += System.nanoTime() - usefulStartTime;

                long syncStartTime = System.nanoTime();
                for (int i = 0; i < vnfInstanceNum; i++) {
                    VNFRunner.getSender(i).writeLocalState(tupleID, readValue);
                }
                aggSyncTime += System.nanoTime() - syncStartTime;

                //TODO: Here we should add lock to all instance, but since OpenNF only has a single controller thread, it is fine

                VNFRequest response = new VNFRequest((int) txnReqId, instanceID, tupleID, 0, timeStamp, request.getPuncID());
                VNFRunner.getSender(instanceID).submitFinishedRequest(response);

            }
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



