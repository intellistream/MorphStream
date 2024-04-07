package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.util.libVNFFrontend.NativeInterface;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OffloadCCThread implements Runnable {
    private final BlockingQueue<byte[]> operationQueue;
    private final ExecutorService offloadExecutor;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();

    public OffloadCCThread(BlockingQueue<byte[]> operationQueue, int writeThreadPoolSize) {
        this.operationQueue = operationQueue;
        this.offloadExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    }


    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] txnByteArray = operationQueue.poll();
            int instanceID = 0;
            long txnReqID = 0;
            int txnType = 1; //TODO: Hardcode, 0:R, 1:W
            if (txnType == 1) {
                try {
                    OutputStream out = instanceSocketMap.get(instanceID).getOutputStream();
                    String combined =  4 + ";" + txnReqID;
                    byte[] byteArray = combined.getBytes();
                    out.write(byteArray);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                offloadExecutor.submit(() -> writeGlobalStates(txnReqID, txnByteArray));

            } else if (txnType == 0) {
                synchronized (this) { //TODO: Consider replace lock with MVCC
                    int txnResult = readGlobalStates(txnReqID, txnByteArray);
                    try {
                        OutputStream out = instanceSocketMap.get(instanceID).getOutputStream();
                        String combined =  4 + ";" + txnReqID + ";" + txnResult; //__txn_finished
                        byte[] byteArray = combined.getBytes();
                        out.write(byteArray);
                        out.flush();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static void writeGlobalStates(long ts, byte[] byteArray) {
        String tableID = "table";
        String tupleID = "0"; //TODO: Hardcoded
        int value = 0;
        try {
            TableRecord condition_record = storageManager.getTable(tableID).SelectKeyRecord(tupleID);
            SchemaRecord srcRecord = condition_record.content_.readPreValues(ts);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);
            tempo_record.getValues().get(1).setInt(value);
            condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    private static int readGlobalStates(long ts, byte[] byteArray) {
        // Check the latest value from DB global store
        int value;
        String tupleID = "0";
        try {
            TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(tupleID);
            value = (int) condition_record.content_.readPreValues(ts).getValues().get(1).getDouble(); //TODO: read the corresponding version, blocking

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
        return value;
    }

    private static void readWriteGlobalStates(long ts, byte[] byteArray) { //TODO: Add support for read-write SAs
//        timeStamp(long) +
//txnReqId(long) +
//tupleID (int) +
//txnIndex(int) +
//saIndex(int) +
//isAbort(int);

        int txnReqID = 0;
        int tupleID = 0;
        int txnIndex = 0;
        int saIndex = 0;
        int isAbort = 0;

//        //stateAccess: saID, type, writeObjIndex, [table name, key's value, field index in table, access type] * N
//        int[] readValues = new int[operation.condition_records.size()]; //<stateObj field value> * N
//
//        int saIndex = 0;
//        for (TableRecord tableRecord : operation.condition_records) {
//            int stateFieldIndex = Integer.parseInt(operation.stateAccess[3 + saIndex * 4 + 2]);
//            SchemaRecord readRecord = tableRecord.content_.readPreValues(operation.bid);
//            readValues[saIndex] = (int) readRecord.getValues().get(stateFieldIndex).getDouble();
//            saIndex++;
//        }
//
//        ByteBuffer byteBuffer = ByteBuffer.allocate(readValues.length * 4);
//        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
//        for (int value : readValues) {
//            byteBuffer.putInt(value);
//        }
//        byte[] readBytes = byteBuffer.array();
//        int isAbort = -1;
//        int udfResult = -1;
//
//        byte[] saResultBytes = NativeInterface._execute_sa_udf(operation.txnReqID, Integer.parseInt(operation.stateAccess[0]), readBytes, readValues.length);
//        isAbort = decodeInt(saResultBytes, 0);
//        udfResult = decodeInt(saResultBytes, 4);
//
//        if (isAbort == 0) { //txn is not aborted
//            if (operation.accessType == CommonMetaTypes.AccessType.WRITE
//                    || operation.accessType == CommonMetaTypes.AccessType.WINDOW_WRITE
//                    || operation.accessType == CommonMetaTypes.AccessType.NON_DETER_WRITE) {
//                //Update udf results to writeRecord
//                SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
//                SchemaRecord tempo_record = new SchemaRecord(srcRecord);
//                tempo_record.getValues().get(1).setDouble(udfResult);
//                operation.d_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);
//            } else {
//                throw new UnsupportedOperationException();
//            }
//        } else if (isAbort == 1) {
//            operation.stateAccess[1] = "true"; //pass isAbort back to bolt
//            operation.isFailed.set(true);
//        } else {
//            throw new RuntimeException();
//        }
    }
}
