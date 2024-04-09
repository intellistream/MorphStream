package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OffloadCCThread implements Runnable {
    private final BlockingQueue<OffloadData> operationQueue;
    private final ExecutorService offloadExecutor;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;

    public OffloadCCThread(BlockingQueue<OffloadData> operationQueue, int writeThreadPoolSize) {
        this.operationQueue = operationQueue;
        this.offloadExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    }

//instanceID(int) -0
//target = 4 (int) -1
//timeStamp(long) -2
//txnReqId(long) -3
//tupleID (int) -4
//txnIndex(int) -5
//saIndex(int) -6
//isAbort(int) -7

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            OffloadData offloadData;
            try {
                offloadData = operationQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            int txnType = 1; //TODO: Hardcode, 0:R, 1:W
            if (txnType == 1) {
                try {
                    OutputStream out = instanceSocketMap.get(offloadData.getInstanceID()).getOutputStream();
                    String combined =  4 + ";" + offloadData.getTxnReqId();
                    byte[] byteArray = combined.getBytes();
                    out.write(byteArray);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                offloadExecutor.submit(() -> writeGlobalStates(offloadData.getTxnReqId(), offloadData.getTupleID(), 0)); //TODO: Value is hardcoded

            } else if (txnType == 0) {
                int txnResult = readGlobalStates(offloadData.getTxnReqId(), offloadData.getTupleID());
                try {
                    OutputStream out = instanceSocketMap.get(offloadData.getInstanceID()).getOutputStream();
                    String combined =  4 + ";" + offloadData.getTxnReqId() + ";" + txnResult; //__txn_finished
                    byte[] byteArray = combined.getBytes();
                    out.write(byteArray);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static void writeGlobalStates(long ts, int tupleID, int value) {
        String tableID = "table"; //TODO: Hardcoded
        try {
            TableRecord condition_record = storageManager.getTable(tableID).SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord srcRecord = condition_record.content_.readPreValues(ts);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);
            tempo_record.getValues().get(1).setInt(value);
            condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    private static int readGlobalStates(long ts, int tupleID) {
        // Check the latest value from DB global store
        int value;
        try {
            TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(String.valueOf(tupleID));
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

    private static long decodeLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (bytes[offset + i] & 0xFF)) << (i * 8);
        }
        return value;
    }

    private static int decodeInt(byte[] bytes, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= (bytes[offset + i] & 0xFF) << (i * 8);
        }
        return value;
    }

    private static List<byte[]> splitByteArray(byte[] byteArray, byte separator) {
        List<byte[]> splitByteArrays = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();

        for (int i = 0; i < byteArray.length; i++) {
            if (byteArray[i] == separator) {
                indexes.add(i);
            }
        }

        int startIndex = 0;
        for (Integer index : indexes) {
            byte[] subArray = new byte[index - startIndex];
            System.arraycopy(byteArray, startIndex, subArray, 0, index - startIndex);
            splitByteArrays.add(subArray);
            startIndex = index + 1;
        }

        // Handling the remaining part after the last occurrence of 59
        if (startIndex < byteArray.length) {
            byte[] subArray = new byte[byteArray.length - startIndex];
            System.arraycopy(byteArray, startIndex, subArray, 0, byteArray.length - startIndex);
            splitByteArrays.add(subArray);
        }

        return splitByteArrays;
    }
}
