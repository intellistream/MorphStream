package intellistream.morphstream.api.input;

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
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;

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
        if (serveRemoteVNF) {
            while (!Thread.currentThread().isInterrupted()) {
                OffloadData request;
                try {
                    request = requestQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (request.getTimeStamp() == -1) {
                    System.out.println("OpenNF CC thread received stop signal");
                    break;
                }
                int saIndex = request.getSaIndex();
                int instanceID = request.getInstanceID();
                int tupleID = request.getTupleID();
                long timeStamp = request.getTimeStamp();
                long txnReqId = request.getTxnReqId();

                TableRecord tableRecord;
                try {
                    tableRecord = storageManager.getTable(saTableNameMap.get(saIndex)).SelectKeyRecord(String.valueOf(tupleID));
                } catch (DatabaseException e) {
                    throw new RuntimeException(e);
                }
                SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                int readValue = readRecord.getValues().get(1).getInt();

                synchronized (instanceLocks.get(instanceID)) {
                    try {
                        AdaptiveCCManager.vnfStubs.get(instanceID).execute_sa_udf(txnReqId, saIndex, tupleID, readValue);
                        SchemaRecord tempo_record = new SchemaRecord(readRecord);
                        tempo_record.getValues().get(1).setInt(readValue);
                        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

                        for (int i = 0; i < vnfInstanceNum; i++) {
                            AdaptiveCCManager.vnfStubs.get(i).update_value(tupleID, readValue);
                        }
                        //TODO: Here we should add lock to all instance, but since OpenNF only has a single controller thread, it is fine

                        AdaptiveCCManager.vnfStubs.get(request.getInstanceID()).txn_handle_done(request.getTxnReqId());

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

        } else {
            throw new RuntimeException("Simulated OpenNF VNF instances not implemented yet");
        }
    }
}



