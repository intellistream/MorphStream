package intellistream.morphstream.transNFV.executors;

import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.state_managers.OffloadMVCCStateManager;
import intellistream.morphstream.transNFV.state_managers.OffloadSVCCStateManager;
import intellistream.morphstream.transNFV.common.Operation;
import intellistream.morphstream.transNFV.common.Transaction;
import intellistream.morphstream.transNFV.vnf.VNFManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class OffloadExecutor implements Runnable {
    private final int offloadExecutorID;
    BlockingQueue<VNFRequest> inputQueue;
    private int requestCounter;
    private int writeCounter;
    private final int doMVCC = MorphStreamEnv.get().configuration().getInt("doMVCC");

    private final HashMap<Integer, OffloadExecutor> offloadExecutors = MorphStreamEnv.get().getTransNFVStateManager().getOffloadExecutors();
    private final OffloadSVCCStateManager svccStateManager;
    private final OffloadMVCCStateManager mvccStateManager;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");

    /** Garbage collection in MVCC */
    private final int gcCheckInterval = MorphStreamEnv.get().configuration().getInt("gcCheckInterval");
    private final int gcBatchInterval = MorphStreamEnv.get().configuration().getInt("gcBatchInterval");
    private HashMap<Integer, Long> batchToTimestamp = new HashMap<>();
    private int currentGCBatchID = 0;
    private int globalCurrentGCBatchID = 0;
    private NonBlockingGCThread gcExecutorThread;

    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private long usefulStartTime = 0;
    private long parsingStartTime = 0;
    private long AGG_USEFUL_TIME = 0;
    private long AGG_PARSING_TIME = 0;

    public OffloadExecutor(int offloadExecutorID, BlockingQueue<VNFRequest> inputQueue,
                           OffloadSVCCStateManager svccStateManager, OffloadMVCCStateManager mvccStateManager) {
        this.offloadExecutorID = offloadExecutorID;
        this.inputQueue = inputQueue;
        this.svccStateManager = svccStateManager;
        this.mvccStateManager = mvccStateManager;
    }

    @Override
    public void run() {
        if (offloadExecutorID == 0 && doMVCC == 1) {
//            gcExecutorThread = new NonBlockingGCThread();
//            Thread gcThread = new Thread(gcExecutorThread);
//            gcThread.start();
        }

        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;
            try {
                request = inputQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (request.getCreateTime() == -1) {
                System.out.println("Offload executor " + offloadExecutorID + " received stop signal. Total requests: " + requestCounter);
//                if (offloadExecutorID == 0 && doMVCC == 1) {
//                    gcExecutorThread.stopGC();
//                    System.out.println("GC thread stopped.");
//                }
                break;
            }

            requestCounter++;
            String type = request.getType();
            if (Objects.equals(type, "Write") || Objects.equals(type, "Read-Write")) {
                writeCounter++;
            }

            try {
                if (doMVCC == 0) {
                    executeSVCCTransaction(request);

                } else if (doMVCC == 1) {
                    executeMVCCTransaction(request);
                    versionManagement(request);

                } else {
                    throw new UnsupportedOperationException();
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            REC_parsingStartTime();
            VNFManager.getInstance(request.getInstanceID()).submitFinishedRequest(request);
            REC_parsingEndTime();
        }
    }

    
    private void executeSVCCTransaction(VNFRequest request) throws InterruptedException {
        int tupleID = request.getTupleID();
        String type = request.getType();
        long timestamp = request.getCreateTime();

        Transaction transaction = constructTransaction(request, type, tupleID, timestamp);

        // Phase 1: Acquire all locks
        Set<Integer> sharedLocks = new HashSet<>();
        Set<Integer> exclusiveLocks = new HashSet<>();
        for (Operation operation : transaction.getOperations()) {
            if (operation.isWrite()) {
                exclusiveLocks.add(operation.getKey());
            } else {
                sharedLocks.add(operation.getKey());
            }
        }
        sharedLocks.removeAll(exclusiveLocks);
        for (int key : sharedLocks) {
            svccStateManager.acquireLock(key, transaction.getTimestamp(), false);
            transaction.getAcquiredLocks().add(key);
        }
        for (int key : exclusiveLocks) {
            svccStateManager.acquireLock(key, transaction.getTimestamp(), true);
            transaction.getAcquiredLocks().add(key);
        }

        // Execute transaction
        REC_usefulStartTime();
        svccStateManager.svccNonBlockingTxnExecution(request);
        REC_usefulEndTime();

        // Phase 2: Release all locks
        for (int key : transaction.getAcquiredLocks()) {
            svccStateManager.releaseLock(key, timestamp);
        }
    }


    private void executeMVCCTransaction(VNFRequest request) throws InterruptedException {
        int tupleID = request.getTupleID();
        String type = request.getType();
        long timestamp = request.getCreateTime();
        Transaction transaction = constructTransaction(request, type, tupleID, timestamp);

        // Sequentially execute operations inside transaction
        for (Operation operation : transaction.getOperations()) {
            long usefulTimePerOperation;
            if (operation.isWrite()) {
                usefulTimePerOperation = mvccStateManager.mvccWrite(request);

            } else {
                usefulTimePerOperation = mvccStateManager.mvccRead(request);
            }
            REC_incrementUsefulTime(usefulTimePerOperation);
        }
    }


    private static Transaction constructTransaction(VNFRequest request, String type, int tupleID, long timestamp) {
        Transaction transaction = new Transaction(request.getCreateTime());
        if (Objects.equals(type, "Read")) {
            transaction.addOperation(tupleID, -1, timestamp,false);
        } else if (Objects.equals(type, "Write")) {
            transaction.addOperation(tupleID, -1, timestamp, true);
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + type);
        }
        return transaction;
    }


    private void versionManagement(VNFRequest request) {
        // Each thread records the end timestamp of each batch
        if (writeCounter % gcBatchInterval == 0) {
            long lastBatchEndTimestamp = request.getCreateTime();
            batchToTimestamp.put(currentGCBatchID, lastBatchEndTimestamp);
            currentGCBatchID++;
        }

        // Thread 0 periodically checks for global finished batch ID and initiates GC
        if (writeCounter > 0 && writeCounter % gcCheckInterval == 0 && offloadExecutorID == 0) {
            int minCurrentBatchID = Integer.MAX_VALUE;

            // Find the minimum finished batch ID among all threads
            for (OffloadExecutor offloadExecutorThread : offloadExecutors.values()) {
                minCurrentBatchID = Math.min(minCurrentBatchID, offloadExecutorThread.getCurrentGCBatchID());
            }

            // If the global finished batch ID is updated, initiate GC
            if (minCurrentBatchID > globalCurrentGCBatchID) {
                globalCurrentGCBatchID = minCurrentBatchID;
                long minBatchEndTimestamp = Long.MAX_VALUE;

                for (OffloadExecutor offloadExecutorThread : offloadExecutors.values()) {
                    minBatchEndTimestamp = Math.min(minBatchEndTimestamp, offloadExecutorThread.getLastBatchEndTimestamp(globalCurrentGCBatchID - 1));
                }

                System.out.println("Offload executor 0 initiates GC for batch " + (globalCurrentGCBatchID - 1) + " with end timestamp " + minBatchEndTimestamp);

                // Garbage collection
                for (int tupleID = 0; tupleID < NUM_ITEMS; tupleID++) {
                    garbageCollection(tupleID, minBatchEndTimestamp);
                }
            }
        }
    }


    private void garbageCollection(int tupleID, long timestamp) {
        try {
            storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID)).content_.garbageCollect(0);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    public int getCurrentGCBatchID() {
        return currentGCBatchID;
    }

    private long getLastBatchEndTimestamp(int batchID) {
        return batchToTimestamp.get(batchID);
    }

    private static void timeout(int microseconds) {
        long startTime = System.nanoTime();
        long waitTime = microseconds * 1000L; // Convert microseconds to nanoseconds
        while (System.nanoTime() - startTime < waitTime) {
            // Busy-wait loop
        }
    }



    private void REC_usefulStartTime() {
        if (enableTimeBreakdown) {
            usefulStartTime = System.nanoTime();
        }
    }

    private void REC_usefulEndTime() {
        if (enableTimeBreakdown) {
            AGG_USEFUL_TIME += System.nanoTime() - usefulStartTime;
        }
    }

    private void REC_incrementUsefulTime(long usefulTime) {
        if (enableTimeBreakdown) {
            AGG_USEFUL_TIME += usefulTime;
        }
    }

    private void REC_parsingStartTime() {
        if (enableTimeBreakdown) {
            parsingStartTime = System.nanoTime();
        }
    }

    private void REC_parsingEndTime() {
        if (enableTimeBreakdown) {
            AGG_PARSING_TIME += System.nanoTime() - parsingStartTime;
        }
    }

    public long getAGG_USEFUL_TIME() {
        return AGG_USEFUL_TIME;
    }

    public long getAGG_PARSING_TIME() {
        return AGG_PARSING_TIME;
    }
    
}
