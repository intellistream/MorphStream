package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class VNFReceiverThread implements Runnable {
    //    Four parallel instances, each maintaining:
//A static CC option
//A range of state partition
//A local hashmap for states
//Two threads (sender and receiver)
//A FIFO queue of completed requests from the state manager
//Some APIs for reading and updating the hashmaps
//A hardcoded VNF function.

//Sender thread
//Read string from CSV file, create request object (reqID, instanceID, tupleID, type)
//Record system time into request object.
//For remote request: forward request to the corresponding CC manager
//For local request: execute local VNF function, forward request to receiver thread

//Receiver thread
//Offer a submitRequest() method for sender / manager to add finished requests
//Take one finished request from the queue at a time
//Record system time into request object
//Finally use a performance calculator tool to compute overall throughput and latency

    private int instanceID;
    private final BlockingQueue<VNFRequest> requestQueue;

    public VNFReceiverThread(int instanceID) {
        this.instanceID = instanceID;
        this.requestQueue = new LinkedBlockingQueue<>();
    }

    public void submitFinishedRequest(VNFRequest request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;
            try {
                request = requestQueue.take();
                request.setFinishTime(System.currentTimeMillis());
                //TODO: Store processed request into a file, or use performance calculate tool to compute overall throughput and latency
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
