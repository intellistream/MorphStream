package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.data.Operation;
import intellistream.morphstream.transNFV.data.Transaction;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;

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
        int type = request.getType();
        long timestamp = request.getCreateTime();

        Transaction transaction = constructTransaction(request, type, tupleID, timestamp);

        // Phase 1: Acquire all locks
        for (Operation operation : transaction.getOperations()) {
            svccStateManager.acquireLock(operation.getKey(), timestamp, operation.isWrite());
            transaction.getAcquiredLocks().add(operation.getKey());
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
        int type = request.getType();
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

        VNFManagerUDF.executeUDF(request); // Simulated UDF execution

//        System.out.println("Transaction completed.");

    }


    private static Transaction constructTransaction(VNFRequest request, int type, int tupleID, long timestamp) {
        Transaction transaction = new Transaction(request.getCreateTime());
        if (type == 0) {
            transaction.addOperation(tupleID, -1, timestamp,false);
        } else if (type == 1) {
            transaction.addOperation(tupleID, -1, timestamp, true);
        } else if (type == 2) {
            transaction.addOperation(tupleID, -1, timestamp, false);
            transaction.addOperation(tupleID, -1, timestamp, true);
        } else {
            throw new UnsupportedOperationException();
        }
        return transaction;
    }


    private void sendACK(VNFRequest request) {
        try {
            request.getTxnACKQueue().put(1); // ACK to instance, notify it to proceed with the next request
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        VNFRunner.getSender(request.getInstanceID()).submitFinishedRequest(request); // register finished req to instance
    }
    
    
}
