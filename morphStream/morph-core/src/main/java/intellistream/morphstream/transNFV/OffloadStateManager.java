package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A global state manager that control concurrent access from Offload Executors to states under Offload-CC
 * */

public class OffloadStateManager implements Runnable {
    private static final ConcurrentHashMap<Integer, Long> lwmMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, ConcurrentSkipListSet<Long>> mvccWriteLockQueues = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, ConcurrentSkipListSet<Long>> svccLockQueues = new ConcurrentHashMap<>();
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();

    // For time breakdown analysis
    private static final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private static final AtomicLong aggSyncTime = new AtomicLong(0); //TODO: This can be optimized by creating separate aggregator for each worker thread
    private static final AtomicLong aggUsefulTime = new AtomicLong(0);
    private static long initEndTime = -1;
    private static long processEndTime = -1;

    // Runtime control signals from VNF
    private static boolean stopSignal = false;

    public OffloadStateManager() {
        for (int i = 0; i < MorphStreamEnv.get().configuration().getInt("NUM_ITEMS"); i++) {
            lwmMap.put(i, 0L);
            mvccWriteLockQueues.put(i, new ConcurrentSkipListSet<>());
            svccLockQueues.put(i, new ConcurrentSkipListSet<>());
        }
    }

    public static int readStateMVCC(VNFRequest request) {
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

    public static void writeStateMVCC(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();
        ConcurrentSkipListSet<Long> lockQueue = mvccWriteLockQueues.get(tupleID);
        if (lockQueue == null) {
            throw new RuntimeException("Lock queue is null");
        }
        lockQueue.add(timeStamp);
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

        long lwm = lwmMap.get(tupleID);
        mvccWriteLockQueues.get(tupleID).remove(lwm);
        //TODO: What if a smaller timestamp arrives late? Then we can getting the wrong min value. Need to buffer and sort for a batch of operations
        maintainLWM(tupleID);
    }


    // Update lwm upon the arrival of each new write request and after the completion of each write request
    private static void maintainLWM(int tupleID) {
        //Thread.sleep(1); //Sleep for 1 millisecond
        ConcurrentSkipListSet<Long> lockQueue = mvccWriteLockQueues.get(tupleID);
        if (lockQueue == null) {
            throw new RuntimeException("Lock queue is null");
        }
        if (lockQueue.isEmpty()) { //no more write request waiting in the queue
            return;
        }
        lwmMap.put(tupleID, lockQueue.first());
    }

    public static int readStateSVCC(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();
        int readValue = -1;
        svccLockQueues.get(tupleID).add(timeStamp);

        while (timeStamp != svccLockQueues.get(tupleID).first()) {
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

    public static void writeStateSVCC(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();
        svccLockQueues.get(tupleID).add(timeStamp);

        while (timeStamp != svccLockQueues.get(tupleID).first()) {
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

    public static void stop() {
        stopSignal = true;
    }

    @Override
    public void run() {
        //TODO: GC for state versions, need to block on-going state access?
        // Time-based or counter-based?
        while (!Thread.currentThread().isInterrupted()) {
            if (stopSignal) {
                break;
            }
        }
    }
}
