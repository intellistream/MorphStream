package intellistream.morphstream.transNFV.executors;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.state_managers.OffloadMVCCStateManager;
import intellistream.morphstream.transNFV.state_managers.OffloadSVCCStateManager;
import intellistream.morphstream.transNFV.common.Operation;
import intellistream.morphstream.transNFV.common.Transaction;
import intellistream.morphstream.transNFV.vnf.UDF;
import intellistream.morphstream.transNFV.vnf.VNFManager;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class OffloadExecutorThread implements Runnable {
    private final int offloadExecutorID;
    BlockingQueue<VNFRequest> inputQueue;
    private int requestCounter;
    private final int doMVCC = MorphStreamEnv.get().configuration().getInt("doMVCC");
    private final OffloadSVCCStateManager svccStateManager = new OffloadSVCCStateManager();
    private final OffloadMVCCStateManager mvccStateManager = new OffloadMVCCStateManager();

    public OffloadExecutorThread(int offloadExecutorID, BlockingQueue<VNFRequest> inputQueue) {
        this.offloadExecutorID = offloadExecutorID;
        this.inputQueue = inputQueue;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;
            try {
                request = inputQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (request.getCreateTime() == -1) {
                System.out.println("Offload executor " + offloadExecutorID + " received stop signal. Total requests: " + requestCounter);
                break;
            }
            requestCounter++;

            try {
                if (doMVCC == 0) {
                    executeSVCCTransaction(request);
                } else if (doMVCC == 1) {
                    executeMVCCTransaction(request);
                } else {
                    throw new UnsupportedOperationException();
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            sendACK(request);
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

        // Simulate transaction execution
        svccStateManager.executeTransaction(request);

        // Phase 2: Release all locks
        for (int key : transaction.getAcquiredLocks()) {
            svccStateManager.releaseLock(key, timestamp);
        }

//        System.out.println("Transaction completed, locks released.");
    }


    private void executeMVCCTransaction(VNFRequest request) throws InterruptedException {
        int tupleID = request.getTupleID();
        String type = request.getType();
        long timestamp = request.getCreateTime();

        Transaction transaction = constructTransaction(request, type, tupleID, timestamp);

        // Sequentially execute operations inside transaction
        for (Operation operation : transaction.getOperations()) {
            if (operation.isWrite()) {
                mvccStateManager.write(tupleID, -1, timestamp);
            } else {
                mvccStateManager.read(tupleID, timestamp);
            }
        }

        UDF.executeUDF(request); // Simulated UDF execution

//        System.out.println("Transaction completed.");

    }


    private static Transaction constructTransaction(VNFRequest request, String type, int tupleID, long timestamp) {
        Transaction transaction = new Transaction(request.getCreateTime());
        if (Objects.equals(type, "Read")) {
            transaction.addOperation(tupleID, -1, timestamp,false);
        } else if (Objects.equals(type, "Write")) {
            transaction.addOperation(tupleID, -1, timestamp, true);
        } else if (Objects.equals(type, "Read-Write")) {
            transaction.addOperation(tupleID, -1, timestamp, false);
            transaction.addOperation(tupleID, -1, timestamp, true);
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + type);
        }
        return transaction;
    }


    private void sendACK(VNFRequest request) {
        try {
            request.getTxnACKQueue().put(1); // ACK to instance, notify it to proceed with the next request
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        VNFManager.getInstance(request.getInstanceID()).submitFinishedRequest(request); // register finished req to instance
    }
    
    
}
