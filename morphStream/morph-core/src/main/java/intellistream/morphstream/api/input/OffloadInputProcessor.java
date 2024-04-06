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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OffloadInputProcessor implements Runnable {
    private final BlockingQueue<byte[]> operationQueue;
    private final ExecutorService offloadExecutor;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();

    public OffloadInputProcessor(BlockingQueue<byte[]> operationQueue, int writeThreadPoolSize) {
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

    private static void writeGlobalStates(long txnID, byte[] byteArray) {
        String tableID = "table";
        String tupleID = "0"; //TODO: Hardcoded
        int value = 0;
        try {
            TableRecord condition_record = storageManager.getTable(tableID).SelectKeyRecord(tupleID);
            SchemaRecord srcRecord = condition_record.content_.readPreValues(txnID);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);
            tempo_record.getValues().get(1).setInt(value);
            condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    private static int readGlobalStates(long txnID, byte[] byteArray) {
        // Check the latest value from DB global store
        int value;
        String tupleID = "0";
        try {
            TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(tupleID);
            value = (int) condition_record.content_.readPreValues(txnID).getValues().get(1).getDouble(); //TODO: read the corresponding version, blocking

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
        return value;
    }
}
