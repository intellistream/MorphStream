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
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class OffloadCCThread implements Runnable {
    private static BlockingQueue<OffloadData> operationQueue;
    private final ExecutorService offloadExecutor;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, Integer> saTypeMap = new HashMap<>();
    private final HashMap<Integer, String> saTableNameMap = new HashMap<>();
    private final Map<Integer, Lock> partitionLocks = new HashMap<>(); //Each table partition holds one lock
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);
    private static final int numPartitions = MorphStreamEnv.get().configuration().getInt("offloadLockNum");
    private static final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each tuple to its lock partition
    private static int requestCounter = 0;
    private final ReentrantLock globalLock = new ReentrantLock();
    private final Condition nextEventCondition = globalLock.newCondition();
    private int watermark = 0;
    private boolean doStatePartitioning = false;


    public OffloadCCThread(BlockingQueue<OffloadData> operationQueue, int writeThreadPoolSize,
                           HashMap<Integer, Integer> saTypeMap, HashMap<Integer, String> saTableNameMap) {
        OffloadCCThread.operationQueue = operationQueue;
        this.offloadExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
        this.saTypeMap.putAll(saTypeMap);
        this.saTableNameMap.putAll(saTableNameMap);
        for (int i = 0; i < numPartitions; i++) {
            partitionLocks.put(i, new ReentrantLock(true));  // Create a fair lock for each partition
        }
        int partitionGap = tableSize / numPartitions;
        for (int i = 0; i < tableSize; i++) {
            partitionOwnership.put(i, i / partitionGap);
        }
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
            while (!Thread.currentThread().isInterrupted()) {
                OffloadData offloadData;
                try {
                    offloadData = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (offloadData.getTimeStamp() == -1) {
                    System.out.println("Offload CC received stop signal.");
                    offloadExecutor.shutdownNow();
                    break; // stop signal received
                }
                offloadData.setLogicalTimeStamp(requestCounter++);
                int saType = saTypeMap.get(offloadData.getSaIndex());
                if (saType == 1) {
                    try {
                        AdaptiveCCManager.vnfStubs.get(offloadData.getInstanceID()).txn_handle_done(offloadData.getTxnReqId());

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
                    System.out.println("Offload CC received stop signal. Total requests: " + requestCounter);
                    offloadExecutor.shutdownNow();
                    break;
                }
                requestCounter++;
                offloadData.setLogicalTimeStamp(requestCounter);
                int saType = offloadData.getSaType();
                if (saType == 1) {
                    offloadData.getSenderResponseQueue().add(1); //Immediate acknowledge write
                    VNFRequest request = new VNFRequest((int) offloadData.getTxnReqId(), offloadData.getInstanceID(),
                            offloadData.getTupleID(), 1, offloadData.getTimeStamp());
                    VNFManager.getReceiver(offloadData.getInstanceID()).submitFinishedRequest(request);
                }

                if (doStatePartitioning) {
                    offloadExecutor.submit(() -> processWithPartitionLock(offloadData));
                } else {
                    offloadExecutor.submit(() -> processWithGlobalLock(offloadData));
                }
            }
        }
    }

    private void processWithPartitionLock(OffloadData offloadData) { //TODO: Ordering needs to be guaranteed
        int tupleID = offloadData.getTupleID();
        int saType = offloadData.getSaType();
        Lock lock = partitionLocks.get(partitionOwnership.get(tupleID));
        lock.lock();
        try {
            if (saType == 1) {
                simOffloadWrite(offloadData);
            } else if (saType == 0 || saType == 2) {
                simOffloadRead(offloadData);
            }
        } finally {
            lock.unlock();
        }
    }

    public void processWithGlobalLock(OffloadData offloadData) {
        boolean processed = false;
        while (!processed) {
            // Check if this event is the next to be processed
            if (offloadData.getLogicalTimeStamp() == watermark + 1) {
                globalLock.lock();
                try {
                    if (offloadData.getLogicalTimeStamp() == watermark + 1) {
                        int saType = offloadData.getSaType();
                        if (saType == 1) {
                            simOffloadWrite(offloadData);
                        } else if (saType == 0 || saType == 2) {
                            simOffloadRead(offloadData);
                        }
                        watermark++;
                        nextEventCondition.signalAll();  // Notify other waiting threads
                        processed = true;  // Mark as processed to break the loop
                    }
                } finally {
                    globalLock.unlock();  // Always release the lock
                }
            } else {
                globalLock.lock();
                try {
                    while (offloadData.getLogicalTimeStamp() != watermark + 1) {
                        nextEventCondition.await();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    globalLock.unlock();
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

            AdaptiveCCManager.vnfStubs.get(offloadData.getInstanceID()).txn_handle_done(txnReqId);

        } catch (DatabaseException | IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void simOffloadWrite(OffloadData offloadData) {
        long timeStamp = offloadData.getTimeStamp();
        int tupleID = offloadData.getTupleID();

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            int udfResult = simUDF(readValue);

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(udfResult);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private void simOffloadRead(OffloadData offloadData) {
        long timeStamp = offloadData.getTimeStamp();
        long txnReqId = offloadData.getTxnReqId();
        int tupleID = offloadData.getTupleID();
        int instanceID = offloadData.getInstanceID();

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            int udfResult = simUDF(readValue);

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(udfResult);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        offloadData.getSenderResponseQueue().add(1);
        VNFRequest request = new VNFRequest((int) txnReqId, instanceID, tupleID, 0, timeStamp); //TODO: Optimization
        VNFManager.getReceiver(instanceID).submitFinishedRequest(request);

    }

    private int simUDF(int tupleValue) throws InterruptedException {
//        Thread.sleep(10);
        //TODO: Simulate UDF better
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
