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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class OffloadCCThread implements Runnable {
    private final BlockingQueue<OffloadData> operationQueue;
    private final ExecutorService offloadExecutor;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, Integer> saTypeMap = new HashMap<>();
    private final HashMap<Integer, String> saTableNameMap = new HashMap<>();

    public OffloadCCThread(BlockingQueue<OffloadData> operationQueue, int writeThreadPoolSize, HashMap<Integer, Integer> saTypeMap, HashMap<Integer, String> saTableNameMap) {
        this.operationQueue = operationQueue;
        this.offloadExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
        this.saTypeMap.putAll(saTypeMap);
        this.saTableNameMap.putAll(saTableNameMap);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            OffloadData offloadData;
            try {
                offloadData = operationQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            OutputStream out;
            int saType = saTypeMap.get(offloadData.getSaIndex());

            if (saType == 1) { //Immediate acknowledge write
                try {
                    out = instanceSocketMap.get(offloadData.getInstanceID()).getOutputStream();
                    String combined =  4 + ";" + offloadData.getTxnReqId();
                    byte[] byteArray = combined.getBytes();
                    out.write(byteArray);
                    out.flush();

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                offloadExecutor.submit(() -> offloadUDF(offloadData));

            } else if (saType == 0 || saType == 2) { //TODO: Consider separating read (from DB) and read-write (execute UDF)?
                try {
                    offloadExecutor.submit(() -> offloadUDF(offloadData));

                    out = instanceSocketMap.get(offloadData.getInstanceID()).getOutputStream();
                    String combined =  4 + ";" + offloadData.getTxnReqId();
                    byte[] byteArray = combined.getBytes();
                    out.write(byteArray);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void offloadUDF(OffloadData offloadData) {

        long timeStamp = offloadData.getTimeStamp();
        long txnReqId = offloadData.getTxnReqId();
        int tupleID = offloadData.getTupleID();
        int txnIndex = offloadData.getTxnIndex();
        int saIndex = offloadData.getSaIndex();

        try {
            TableRecord tableRecord = storageManager.getTable(saTableNameMap.get(saIndex)).SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp); //TODO: Blocking until record is available, wait for a timeout?

            int readValue = (int) readRecord.getValues().get(1).getDouble();
            ByteBuffer byteBuffer = ByteBuffer.allocate(1);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.putInt(readValue);
            byte[] readBytes = byteBuffer.array();
            int udfResult = -1;

            byte[] saResultBytes = NativeInterface._execute_sa_udf(txnReqId, txnIndex, saIndex, readBytes, 1); //TODO: Add txnIndex as well?op
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
