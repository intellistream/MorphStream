package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

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
    private static BlockingQueue<VNFRequest> operationQueue;
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
    private boolean doStatePartitioning = true;
    private static final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private static final AtomicLong aggSyncTime = new AtomicLong(0); //TODO: This can be optimized by creating separate aggregator for each worker thread
    private static final AtomicLong aggUsefulTime = new AtomicLong(0);


    public OffloadCCThread(BlockingQueue<VNFRequest> operationQueue, int writeThreadPoolSize) {
        OffloadCCThread.operationQueue = operationQueue;
        this.offloadExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
        for (int i = 0; i < numPartitions; i++) {
            partitionLocks.put(i, new ReentrantLock(true));  // Create a fair lock for each partition
        }
        int partitionGap = tableSize / numPartitions; // 10000/1000 = 10
        for (int i = 0; i < tableSize; i++) {
            partitionOwnership.put(i, i / partitionGap);
        }
    }

    public static void submitOffloadReq(VNFRequest request) {
        try {
            operationQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {

        if (communicationChoice == 1) {
            throw new UnsupportedOperationException();

        } else if (communicationChoice == 0) {
            while (!Thread.currentThread().isInterrupted()) {
                VNFRequest request;
                try {
                    request = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (request.getCreateTime() == -1) {
                    System.out.println("Offload CC received stop signal. Total requests: " + requestCounter);
                    offloadExecutor.shutdownNow();
                    break;
                }
                requestCounter++;
                request.setLogicalTS(requestCounter);
                int saType = request.getType();
                if (saType == 1) {
                    try {
                        request.getTxnACKQueue().put(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    VNFRunner.getSender(request.getInstanceID()).submitFinishedRequest(request); //Send txn_finish signal to instance
                }

                if (doStatePartitioning) {
                    offloadExecutor.submit(() -> simProcessPartitionLock(request));
                } else {
                    offloadExecutor.submit(() -> simProcessGlobalLock(request));
                }
            }
        }
    }

    private void simProcessPartitionLock(VNFRequest offloadData) { //TODO: Ordering needs to be guaranteed
        int tupleID = offloadData.getTupleID();
        int saType = offloadData.getType();

        long syncStartTime = System.nanoTime();
        Lock lock = partitionLocks.get(partitionOwnership.get(tupleID));
        lock.lock();
        if (enableTimeBreakdown) {
            aggSyncTime.addAndGet(System.nanoTime() - syncStartTime);
        }

        try {
            long usefulStartTime = System.nanoTime();
            if (saType == 1) {
                simOffloadWrite(offloadData);
            } else if (saType == 0 || saType == 2) {
                simOffloadRead(offloadData);
            }
            if (enableTimeBreakdown) {
                aggUsefulTime.addAndGet(System.nanoTime() - usefulStartTime);
            }

        } finally {
            long syncStartTime2 = System.nanoTime();
            lock.unlock();
            if (enableTimeBreakdown) {
                aggSyncTime.addAndGet(System.nanoTime() - syncStartTime2);
            }
        }
    }

    public void simProcessGlobalLock(VNFRequest offloadData) {
        boolean processed = false;
        while (!processed) {
            // Check if this event is the next to be processed
            if (offloadData.getLogicalTS() == watermark + 1) {
                long syncStartTime = System.nanoTime();
                globalLock.lock();
                aggSyncTime.addAndGet(System.nanoTime() - syncStartTime);

                try {
                    if (offloadData.getLogicalTS() == watermark + 1) {
                        long usefulStartTime = System.nanoTime();
                        int saType = offloadData.getType();
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
                    while (offloadData.getLogicalTS() != watermark + 1) {
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

    private void simOffloadWrite(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            VNFManagerUDF.executeUDF(request);
            readValue+=1;

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(readValue);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }

    }

    private void simOffloadRead(VNFRequest request) {
        long timeStamp = request.getCreateTime();
        int tupleID = request.getTupleID();
        int instanceID = request.getInstanceID();

        try {
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            VNFManagerUDF.executeUDF(request);
            readValue+=1;

            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(readValue);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }

        try {
            request.getTxnACKQueue().put(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        VNFRunner.getSender(instanceID).submitFinishedRequest(request);

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
