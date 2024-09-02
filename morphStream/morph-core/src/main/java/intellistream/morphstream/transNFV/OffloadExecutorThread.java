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
//            request.setLogicalTS(requestCounter); //TODO: We should inherit timestamp from request
            
            if (doMVCC == 0) {
                try {
                    executeSVCCTransaction(request);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new UnsupportedOperationException();
            }
            
            sendACK(request);
            
        }
    }
    
    private void executeSVCCTransaction(VNFRequest request) throws InterruptedException {
        int tupleID = request.getTupleID();
        int type = request.getType();
        long timestamp = request.getCreateTime();

        Transaction transaction = constructTransaction(request, type, tupleID);

        for (Operation operation : transaction.getOperations()) {
            svccStateManager.acquireLock(operation.getKey(), timestamp, operation.isWrite());
            transaction.getAcquiredLocks().add(operation.getKey());
        }

        // Simulate transaction execution
        svccStateManager.executeTransaction(request);

        // Phase 2: Release all locks
        for (String key : transaction.getAcquiredLocks()) {
            svccStateManager.releaseLock(key, timestamp);
        }

        System.out.println("Transaction completed, locks released.");
    }

    private static Transaction constructTransaction(VNFRequest request, int type, int tupleID) {
        Transaction transaction = new Transaction(request.getCreateTime());
        if (type == 0) {
            transaction.addOperation(String.valueOf(tupleID), false);
        } else if (type == 1) {
            transaction.addOperation(String.valueOf(tupleID), true);
        } else if (type == 2) {
            transaction.addOperation(String.valueOf(tupleID), false);
            transaction.addOperation(String.valueOf(tupleID), true);
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
