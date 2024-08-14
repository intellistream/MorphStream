package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.util.HashMap;
import java.util.PriorityQueue;

/**
 * A global state manager that control concurrent access from Offload Executors to states under Offload-CC
 * */

public class OffloadStateManager implements Runnable {
    private final HashMap<Integer, Long> lwmMap = new HashMap<>();
    private final HashMap<Integer, PriorityQueue<Long>> mvccWriteLockQueues = new HashMap<>();
    private final HashMap<Integer, PriorityQueue<Long>> svccLockQueues = new HashMap<>();
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();

    public int readStateMVCC(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();
        int readValue = -1;

        while (request.getCreateTime() >= lwmMap.get(tupleID)) {
            // blocking wait
        }

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            readValue = readRecord.getValues().get(1).getInt();
            VNFManagerUDF.executeUDF(request);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }

        return readValue;
    }

    public void writeStateMVCC(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();
        mvccWriteLockQueues.get(tupleID).add(timeStamp);
        maintainLWM(tupleID);

        while (timeStamp != lwmMap.get(tupleID)) {
            // blocking wait
        }

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            VNFManagerUDF.executeUDF(request);
            readValue += 1;

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(readValue);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }

        svccLockQueues.get(tupleID).poll(); //TODO: make sure to remove the ts of the operation that has just been processed.
        //TODO: GPT suggests using combination of treemap and hashmap to locate the min item and remove it accurately.
        //TODO: What if a smaller timestamp arrives late? Then we can getting the wrong min value.
        maintainLWM(tupleID);
    }

    // Update lwm upon the arrival of each new write request and after the completion of each write request
    private void maintainLWM(int tupleID) {
        //Thread.sleep(1); //Sleep for 1 millisecond
        lwmMap.put(tupleID, mvccWriteLockQueues.get(tupleID).peek());
    }

    public int readStateSVCC(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();
        int readValue = -1;
        svccLockQueues.get(tupleID).add(timeStamp);

        while (timeStamp != svccLockQueues.get(tupleID).peek()) {
            // blocking wait
        }

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            readValue = readRecord.getValues().get(1).getInt();
            VNFManagerUDF.executeUDF(request);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }

        return readValue;
    }

    public void writeStateSVCC(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();
        svccLockQueues.get(tupleID).add(timeStamp);

        while (timeStamp != svccLockQueues.get(tupleID).peek()) {
            // blocking wait
        }

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            VNFManagerUDF.executeUDF(request);
            readValue += 1;

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(readValue);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        //TODO: GC for state versions, might block on-going state access
        // Time-based or counter-based?

    }
}
