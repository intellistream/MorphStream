package intellistream.morphstream.api.input.openNF;

import intellistream.morphstream.api.input.CacheData;
import intellistream.morphstream.api.input.java_peer.src.main.java.message.VNFCtrlClient;

import java.util.concurrent.BlockingQueue;

// ControllerThread class representing a controller thread for handling a dedicated VNF
class OpenNFControllerThread implements Runnable {
    private final int vnfId;
    private final int vnfParallelism;
    private final BlockingQueue<CacheData> requestQueue; // FIFO queue of shared-state access requests (R/W) for this VNF

    public OpenNFControllerThread(int vnfId, int vnfParallelism, BlockingQueue<CacheData> requestQueue) {
        this.vnfId = vnfId;
        this.vnfParallelism = vnfParallelism;
        this.requestQueue = requestQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                CacheData request = requestQueue.take();
                int instanceID = request.getInstanceID();
                int tupleID = request.getTupleID();
                int value = request.getValue();

                for (int i = 0; i < vnfParallelism; i++) {
                    if (i != instanceID) {
                        System.out.println("ControllerThread " + vnfId + " sent state update to instance " + i);
                        VNFCtrlClient.update_value(tupleID, value);
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
