package intellistream.morphstream.api.input;

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
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private static final ConcurrentHashMap<Integer, Integer> tupleOwnership = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> fetchedValues = MorphStreamEnv.fetchedValues;

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
        if (serveRemoteVNF) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    OffloadData request;
                    request = requestQueue.take();
                    if (request.getTimeStamp() == -1) {
                        System.out.println("CHC CC thread received stop signal");
                        break;
                    }
                    int saIndex = request.getSaIndex();
                    int instanceID = request.getInstanceID();
                    int tupleID = request.getTupleID();
                    long timeStamp = request.getTimeStamp();
                    long txnReqId = request.getTxnReqId();

                    if (tupleOwnership.get(tupleID) == null) {
                        tupleOwnership.put(tupleID, instanceID);

                    } else if (tupleOwnership.get(tupleID) == instanceID) {
                        System.out.println("Tuple " + tupleID + " is still owned by instance " + instanceID);
                        synchronized (instanceLocks.get(instanceID)) {
                            AdaptiveCCManager.vnfStubs.get(instanceID).txn_handle_done(txnReqId);
                        }

                    } else {
                        System.out.println("Tuple " + tupleID + " is accessed by another instance " + instanceID);
                        int currentOwner = tupleOwnership.get(tupleID);

                        // Fetch tuple value from the current owner instance
                        synchronized (instanceLocks.get(currentOwner)) {
                            AdaptiveCCManager.vnfStubs.get(currentOwner).fetch_value(tupleID);
                        }
                        while (!fetchedValues.containsKey(tupleID)) {
                            try {
                                // Avoid busy-waiting
                                TimeUnit.MILLISECONDS.sleep(50); //TODO: Make sure it does not introduce bottleneck
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                        int tupleValue = fetchedValues.get(tupleID);
                        fetchedValues.remove(tupleID);

                        // Execute SA UDF on the fetched value
                        synchronized (instanceLocks.get(instanceID)) {
                            AdaptiveCCManager.vnfStubs.get(instanceID).execute_sa_udf(txnReqId, saIndex, tupleID, tupleValue);
                        }

                        // Update tuple value into global storage
                        TableRecord tableRecord = storageManager.getTable(saTableNameMap.get(saIndex)).SelectKeyRecord(String.valueOf(tupleID));
                        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
                        SchemaRecord tempo_record = new SchemaRecord(readRecord);
                        tempo_record.getValues().get(1).setInt(tupleValue);
                        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
                    }

                } catch (InterruptedException e) {
                    System.out.println("CHC Interrupted exception: " + e.getMessage());
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    System.out.println("CHC IO exception: " + e.getMessage());
                    throw new RuntimeException(e);
                } catch (DatabaseException e) {
                    System.out.println("CHC Database exception: " + e.getMessage());
                    throw new RuntimeException(e);
                } catch (RuntimeException e) {
                    System.out.println("CHC Runtime exception: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }

        } else {
            throw new RuntimeException("Simulated OpenNF VNF instances not implemented yet");
        }
    }
}
