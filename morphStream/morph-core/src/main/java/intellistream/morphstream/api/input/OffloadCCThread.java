package intellistream.morphstream.api.input;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class OffloadCCThread implements Runnable {
    private static BlockingQueue<OffloadData> operationQueue;
    private final ExecutorService offloadExecutor;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, Integer> saTypeMap = MorphStreamEnv.get().getSaTypeMap();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final Map<Integer, Lock> partitionLocks = new HashMap<>(); //Each table partition holds one lock
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final int numPartitions = MorphStreamEnv.get().configuration().getInt("offloadLockNum");
    private static final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each tuple to its lock partition
    private static int requestCounter = 0;
    private final ReentrantLock globalLock = new ReentrantLock();
    private final Condition nextEventCondition = globalLock.newCondition();
    private int watermark = 0;
    private boolean doStatePartitioning = false;
    private static final AtomicLong aggSyncTime = new AtomicLong(0); //TODO: This can be optimized by creating separate aggregator for each worker thread
    private static final AtomicLong aggUsefulTime = new AtomicLong(0);


    public OffloadCCThread(BlockingQueue<OffloadData> operationQueue, int writeThreadPoolSize) {
        OffloadCCThread.operationQueue = operationQueue;
        this.offloadExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
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

        if (communicationChoice == 1) {
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
                    synchronized (instanceLocks.get(offloadData.getInstanceID())) {
                        try {
                            AdaptiveCCManager.vnfStubs.get(offloadData.getInstanceID()).txn_handle_done(offloadData.getTxnReqId());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    offloadExecutor.submit(() -> offloadWrite(offloadData));

                } else if (saType == 0 || saType == 2) {
                    offloadExecutor.submit(() -> offloadRead(offloadData));
                }

            }

        } else if (communicationChoice == 0) {
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
                    VNFRequest request = new VNFRequest((int) offloadData.getTxnReqId(), offloadData.getInstanceID(),
                            offloadData.getTupleID(), 1, offloadData.getTimeStamp());
                    VNFRunner.getSender(offloadData.getInstanceID()).submitFinishedRequest(request); //Send txn_finish signal to instance
                }

                if (doStatePartitioning) {
                    offloadExecutor.submit(() -> simProcessPartitionLock(offloadData));
                } else {
                    offloadExecutor.submit(() -> simProcessGlobalLock(offloadData));
                }
            }
        }
    }

    private void simProcessPartitionLock(OffloadData offloadData) { //TODO: Ordering needs to be guaranteed
        int tupleID = offloadData.getTupleID();
        int saType = offloadData.getSaType();

        long syncStartTime = System.nanoTime();
        Lock lock = partitionLocks.get(partitionOwnership.get(tupleID));
        lock.lock();
        aggSyncTime.addAndGet(System.nanoTime() - syncStartTime);

        try {
            long usefulStartTime = System.nanoTime();
            if (saType == 1) {
                simOffloadWrite(offloadData);
            } else if (saType == 0 || saType == 2) {
                simOffloadRead(offloadData);
            }
            aggUsefulTime.addAndGet(System.nanoTime() - usefulStartTime);

        } finally {
            long syncStartTime2 = System.nanoTime();
            lock.unlock();
            aggSyncTime.addAndGet(System.nanoTime() - syncStartTime2);
        }
    }

    public void simProcessGlobalLock(OffloadData offloadData) {
        boolean processed = false;
        while (!processed) {
            // Check if this event is the next to be processed
            if (offloadData.getLogicalTimeStamp() == watermark + 1) {
                long syncStartTime = System.nanoTime();
                globalLock.lock();
                aggSyncTime.addAndGet(System.nanoTime() - syncStartTime);

                try {
                    if (offloadData.getLogicalTimeStamp() == watermark + 1) {
                        long usefulStartTime = System.nanoTime();
                        int saType = offloadData.getSaType();
                        if (saType == 1) {
                            simOffloadWrite(offloadData);
                        } else if (saType == 0 || saType == 2) {
                            simOffloadRead(offloadData);
                        }
                        aggUsefulTime.addAndGet(System.nanoTime() - usefulStartTime);
                        watermark++;
                        nextEventCondition.signalAll();  // Notify other waiting threads
                        processed = true;  // Mark as processed to break the loop
                    }
                } finally {
                    globalLock.unlock();  // Always release the lock
                }
            } else {
                long syncStartTime = System.nanoTime(); //TODO: Separate Lock and Sync (awaiting watermark) into two categories?
                globalLock.lock();
                aggSyncTime.addAndGet(System.nanoTime() - syncStartTime);

                try {
                    long syncStartTime2 = System.nanoTime();
                    while (offloadData.getLogicalTimeStamp() != watermark + 1) {
                        nextEventCondition.await();
                    }
                    aggSyncTime.addAndGet(System.nanoTime() - syncStartTime2);

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
        int instanceID = offloadData.getInstanceID();

        try {
            TableRecord tableRecord = storageManager.getTable(saTableNameMap.get(saIndex)).SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            try {
                synchronized (instanceLocks.get(instanceID)) {
                    AdaptiveCCManager.vnfStubs.get(instanceID).execute_sa_udf(txnReqId, saIndex, tupleID, readValue);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(readValue);
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
        int instanceID = offloadData.getInstanceID();

        try {
            TableRecord tableRecord = storageManager.getTable(saTableNameMap.get(saIndex)).SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp); //TODO: Blocking until record is available, wait for a timeout?
            int readValue = readRecord.getValues().get(1).getInt();
            try {
                synchronized (instanceLocks.get(instanceID)) {
                    AdaptiveCCManager.vnfStubs.get(instanceID).execute_sa_udf(txnReqId, saIndex, tupleID, readValue);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(readValue);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
            synchronized (instanceLocks.get(instanceID)) {
                AdaptiveCCManager.vnfStubs.get(offloadData.getInstanceID()).txn_handle_done(txnReqId);
            }

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

        VNFRequest request = new VNFRequest((int) txnReqId, instanceID, tupleID, 0, timeStamp); //TODO: Optimization
        VNFRunner.getSender(instanceID).submitFinishedRequest(request);

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

    public static AtomicLong getAggSyncTime() {
        return aggSyncTime;
    }

    public static AtomicLong getAggUsefulTime() {
        return aggUsefulTime;
    }
}
