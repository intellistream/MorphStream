package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;

import java.util.concurrent.BlockingQueue;

public class OffloadExecutor implements Runnable {

    private final int offloadExecutorID;
    //TODO: Get input queue from VNF instances, similar to morph bolts
    BlockingQueue<VNFRequest> operationQueue;
    private int requestCounter;
    private final int doMVCC = 1; //TODO: hardcoded

    public OffloadExecutor(int offloadExecutorID, BlockingQueue<VNFRequest> operationQueue) {
        this.offloadExecutorID = offloadExecutorID;
        this.operationQueue = operationQueue;
    }

    private void sendACK(VNFRequest request) {
        try {
            request.getTxnACKQueue().put(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        VNFRunner.getSender(request.getInstanceID()).submitFinishedRequest(request); //Early ACK
    }

    @Override
    public void run() {
//        initEndTime = System.nanoTime();

        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;
            try {
                request = operationQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (request.getCreateTime() == -1) {
//                processEndTime = System.nanoTime();
//                writeCSVTimestamps();
                System.out.println("Offload executor " + offloadExecutorID + " received stop signal. Total requests: " + requestCounter);
                break;
            }

            requestCounter++;
            request.setLogicalTS(requestCounter);
            int saType = request.getType();

            if (saType == 1) {
                sendACK(request); // Early ACK for write operations

                if (doMVCC == 0) {
                    OffloadStateManager.writeStateSVCC(request);
                } else if (doMVCC == 1) {
                    OffloadStateManager.writeStateMVCC(request);
                } else {
                    throw new UnsupportedOperationException();
                }

            } else {
                if (doMVCC == 0) {
                    OffloadStateManager.readStateSVCC(request);
                } else if (doMVCC == 1) {
                    OffloadStateManager.readStateMVCC(request);
                } else {
                    throw new UnsupportedOperationException();
                }
                sendACK(request); // Normal ACK for read operations
            }

        }
    }

}
