package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
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

    private static void readWriteGlobalStates(OffloadData offloadData) { //TODO: Specify Read-only, write-only or read-write?

        int instanceID = offloadData.getInstanceID();
        long timeStamp = offloadData.getTimeStamp();
        long txnReqId = offloadData.getTxnReqId();
        int tupleID = offloadData.getTupleID();
        int txnIndex = offloadData.getTxnIndex();
        int saIndex = offloadData.getSaIndex();

        try {
            TableRecord tableRecord = storageManager.getTable("table").SelectKeyRecord(String.valueOf(tupleID)); //TODO: Add table name
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = (int) readRecord.getValues().get(1).getDouble();

            ByteBuffer byteBuffer = ByteBuffer.allocate(1);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.putInt(readValue);
            byte[] readBytes = byteBuffer.array();
            int udfResult = -1;

            byte[] saResultBytes = NativeInterface._execute_sa_udf(txnReqId, saIndex, readBytes, 1); //TODO: Add txnIndex as well?
            udfResult = decodeInt(saResultBytes, 4);

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(udfResult);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }

    }

    private static int decodeInt(byte[] bytes, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= (bytes[offset + i] & 0xFF) << (i * 8);
        }
        return value;
    }

}
