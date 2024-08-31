package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class OffloadCCExecutorService implements Runnable {
    private static BlockingQueue<VNFRequest> operationQueue;
    private final ExecutorService offloadExecutor;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, Integer> saTypeMap = MorphStreamEnv.get().getSaTypeMap();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final Map<Integer, Lock> partitionLocks = new HashMap<>(); //Each table partition holds one lock
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final int numPartitions = MorphStreamEnv.get().configuration().getInt("offloadLockNum");
    private static final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each tuple to its lock partition
    private static int requestCounter = 0;
    private boolean doStatePartitioning = true;
    private static final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private static final AtomicLong aggSyncTime = new AtomicLong(0); //TODO: This can be optimized by creating separate aggregator for each worker thread
    private static final AtomicLong aggUsefulTime = new AtomicLong(0);
    private static long initEndTime = -1;
    private static long processEndTime = -1;


    public OffloadCCExecutorService(BlockingQueue<VNFRequest> operationQueue, int writeThreadPoolSize) {
        OffloadCCExecutorService.operationQueue = operationQueue;
        this.offloadExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
        for (int i = 0; i < numPartitions; i++) {
            partitionLocks.put(i, new ReentrantLock(true));  // Create a fair lock for each partition
        }
        int partitionGap = tableSize / numPartitions;
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
        initEndTime = System.nanoTime();

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
                    processEndTime = System.nanoTime();
                    writeCSVTimestamps();
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
                    throw new UnsupportedOperationException();
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

    private static void writeCSVTimestamps() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "timestamps");
        String filePath = String.format("%s/%s.csv", baseDirectory, "Offloading");
        System.out.println("Writing to " + filePath);
        File dir = new File(baseDirectory);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return;
            }
        }
        File file = new File(filePath);
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                System.out.println("Failed to delete existing file.");
                return;
            }
        }
        try (FileWriter fileWriter = new FileWriter(file)) {
            String lineToWrite = initEndTime + "," + processEndTime + "\n";
            fileWriter.write(lineToWrite);
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
        }
    }

    public static AtomicLong getAggSyncTime() {
        return aggSyncTime;
    }
    public static AtomicLong getAggUsefulTime() {
        return aggUsefulTime;
    }
    public static long getInitEndTime() {
        return initEndTime;
    }
    public static long getProcessEndTime() {
        return processEndTime;
    }
}
