import libVNFFrontend.NativeInterface;

import java.util.concurrent.BlockingQueue;

// ControllerThread class representing a controller thread for handling a dedicated VNF
class OpenNFControllerThread implements Runnable {
    private final int vnfId;
    private final int vnfParallelism;
    private final BlockingQueue<String> requestQueue; // FIFO queue of shared-state access requests (R/W) for this VNF

    public OpenNFControllerThread(int vnfId, int vnfParallelism, BlockingQueue<String> requestQueue) {
        this.vnfId = vnfId;
        this.vnfParallelism = vnfParallelism;
        this.requestQueue = requestQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                String request = requestQueue.take();
                int instanceID = Integer.parseInt(request.split(",")[0]);
                System.out.println("ControllerThread " + vnfId + " received request from instance " + instanceID + ", state update: " + request);
                String stateUpdate = NativeInterface.__process_request(instanceID, request);

                for (int i = 0; i < vnfParallelism; i++) {
                    if (i != instanceID) {
                        System.out.println("ControllerThread " + vnfId + " sent state update to instance " + i);
                        NativeInterface.__set_per_flow_state(i, stateUpdate);
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
