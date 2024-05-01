package intellistream.morphstream.api.input;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.simVNF.VNFManager;
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
import java.util.concurrent.atomic.AtomicInteger;


public class OffloadCCThread implements Runnable {
    private static BlockingQueue<OffloadData> operationQueue;
    private final ExecutorService offloadExecutor;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, Integer> saTypeMap = new HashMap<>();
    private final HashMap<Integer, String> saTableNameMap = new HashMap<>();
    private AtomicInteger requestCount = new AtomicInteger(0);
    private final int expRequestCount;
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);

    public OffloadCCThread(BlockingQueue<OffloadData> operationQueue, int writeThreadPoolSize,
                           HashMap<Integer, Integer> saTypeMap, HashMap<Integer, String> saTableNameMap, int expRequestCount) {
        OffloadCCThread.operationQueue = operationQueue;
        this.expRequestCount = expRequestCount;
        this.offloadExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
        this.saTypeMap.putAll(saTypeMap);
        this.saTableNameMap.putAll(saTableNameMap);
    }

    public static void submitOffloadReq(OffloadData offloadData) {
        try {
            operationQueue.put(offloadData);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {

        if (serveRemoteVNF) {
            OutputStream out;
            while (!Thread.currentThread().isInterrupted()) {
                OffloadData offloadData;
                try {
                    offloadData = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (offloadData.getTimeStamp() == -1) {
                    System.out.println("Offload CC received stop signal.");
                    break; // stop signal received
                }
                int saType = saTypeMap.get(offloadData.getSaIndex());
                if (saType == 1) {
                    try {
                        out = instanceSocketMap.get(offloadData.getInstanceID()).getOutputStream(); //Immediate acknowledge write
                        String combined =  4 + ";" + offloadData.getTxnReqId();
                        byte[] byteArray = combined.getBytes();
                        out.write(byteArray);
                        out.flush();

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    offloadExecutor.submit(() -> offloadWrite(offloadData));

                } else if (saType == 0 || saType == 2) {
                    offloadExecutor.submit(() -> offloadRead(offloadData));
                }
            }

        } else {
            while (!Thread.currentThread().isInterrupted()) {
                OffloadData offloadData;
                try {
                    offloadData = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (offloadData.getTimeStamp() == -1) {
                    System.out.println("Offload CC received stop signal.");
                    break; // stop signal received
                }
                int saType = offloadData.getSaType();

                if (saType == 1) {
                    VNFRequest request = new VNFRequest((int) offloadData.getTxnReqId(), offloadData.getInstanceID(),
                            offloadData.getTupleID(), 1, offloadData.getTimeStamp());
                    VNFManager.getReceiver(offloadData.getInstanceID()).submitFinishedRequest(request);
                    offloadExecutor.submit(() -> simOffloadWrite(offloadData));

                } else if (saType == 0 || saType == 2) {
                    offloadExecutor.submit(() -> simOffloadRead(offloadData));
                }
            }
        }


    }

    private void offloadWrite(OffloadData offloadData) {

        long timeStamp = offloadData.getTimeStamp();
        long txnReqId = offloadData.getTxnReqId();
        int tupleID = offloadData.getTupleID();
        int saIndex = offloadData.getSaIndex();

        try {
            TableRecord tableRecord = storageManager.getTable(saTableNameMap.get(saIndex)).SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);

            int readValue = (int) readRecord.getValues().get(1).getDouble();
            ByteBuffer byteBuffer = ByteBuffer.allocate(1);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.putInt(readValue);
            byte[] readBytes = byteBuffer.array();
            int udfResult = -1;

            byte[] saResultBytes = NativeInterface._execute_sa_udf(txnReqId, saIndex, readBytes, 1);
            udfResult = decodeInt(saResultBytes, 4);

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(udfResult);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }

    }

    private void offloadRead(OffloadData offloadData) {

        long timeStamp = offloadData.getTimeStamp();
        long txnReqId = offloadData.getTxnReqId();
        int tupleID = offloadData.getTupleID();
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

            byte[] saResultBytes = NativeInterface._execute_sa_udf(txnReqId, saIndex, readBytes, 1);
            udfResult = decodeInt(saResultBytes, 4);

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(udfResult);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

            OutputStream out = instanceSocketMap.get(offloadData.getInstanceID()).getOutputStream();
            String combined =  4 + ";" + offloadData.getTxnReqId();
            byte[] byteArray = combined.getBytes();
            out.write(byteArray);
            out.flush();

        } catch (DatabaseException | IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void simOffloadWrite(OffloadData offloadData) {

        long timeStamp = offloadData.getTimeStamp();
        int tupleID = offloadData.getTupleID();

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp); //TODO: Blocking until record is available, wait for a timeout?

            int readValue = readRecord.getValues().get(1).getInt();
            int udfResult = simUDF(readValue);

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(udfResult);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (requestCount.incrementAndGet() == expRequestCount) {
            System.out.println("Offload CC completed all " + expRequestCount + " requests.");
        }

    }

    private void simOffloadRead(OffloadData offloadData) {

        long timeStamp = offloadData.getTimeStamp();
        long txnReqId = offloadData.getTxnReqId();
        int tupleID = offloadData.getTupleID();
        int instanceID = offloadData.getInstanceID();

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp); //TODO: Blocking until record is available, wait for a timeout?

            int readValue = readRecord.getValues().get(1).getInt();
            int udfResult = simUDF(readValue);

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(udfResult);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

            VNFRequest request = new VNFRequest((int) txnReqId, instanceID, tupleID, 0, timeStamp); //TODO: Optimization
            VNFManager.getReceiver(instanceID).submitFinishedRequest(request);

        } catch (DatabaseException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (requestCount.incrementAndGet() == expRequestCount) {
            System.out.println("Offload CC completed all " + expRequestCount + " requests.");
        }

//        System.out.println("Offload read completed.");

    }

    private int simUDF(int tupleValue) throws InterruptedException {
        Thread.sleep(10);
        return tupleValue;
    }

    private static int decodeInt(byte[] bytes, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= (bytes[offset + i] & 0xFF) << (i * 8);
        }
        return value;
    }

}
