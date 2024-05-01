package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.util.concurrent.*;

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

    private final int instanceID;
    private final BlockingQueue<VNFRequest> requestQueue;
    private ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = AdaptiveCCManager.tpgQueues;
    private final CyclicBarrier finishBarrier;
    private final int expRequestCount;
    private int actualRequestCount = 0;
    private long startTime;
    private long endTime;

    public VNFReceiverThread(int instanceID, int expRequestCount, CyclicBarrier finishBarrier) {
        this.instanceID = instanceID;
        this.expRequestCount = expRequestCount;
        this.finishBarrier = finishBarrier;
        this.requestQueue = new LinkedBlockingQueue<>();
    }

    public void submitFinishedRequest(VNFRequest request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public int getActualRequestCount() {
        return actualRequestCount;
    }
    public long getStartTime() {
        return startTime;
    }
    public long getEndTime() {
        return endTime;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("VNF receiver instance " + instanceID + " started.");
        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;
            try {
                request = requestQueue.take();
                request.setFinishTime(System.currentTimeMillis());
                actualRequestCount++;
                //TODO: Store processed request into a file, or use performance calculate tool to compute overall throughput and latency
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (actualRequestCount == expRequestCount) {
                endTime = System.nanoTime();
                startTime = VNFManager.getSenderMap().get(instanceID).getStartTime();
                long durationNano = endTime - startTime;
                System.out.println("VNF receiver " + instanceID + " processed all " + expRequestCount + " requests with throughput " + (actualRequestCount / (durationNano / 1E9)) + " events/second");
                MorphStreamEnv.get().simVNFLatch.countDown();

                try {
                    int arrivedIndex = finishBarrier.await();
                    if (arrivedIndex == 0) {
                        System.out.println("All VNF receivers have finished processing requests, sending stop signals...");
                        MonitorThread.submitPatternData(new PatternData(-1, instanceID, 0, false));
                        PartitionCCThread.submitPartitionRequest(new PartitionData(-1, 0, instanceID, 0, 0));
                        CacheCCThread.submitReplicationRequest(new CacheData(-1, 0, instanceID, 0));
                        OffloadCCThread.submitOffloadReq(new OffloadData(-1, instanceID, 0, 0, 0, 0, 0, 0));
                        for (int tpgQueueIndex = 0; tpgQueueIndex < tpgQueues.size(); tpgQueueIndex++) {
                            tpgQueues.get(tpgQueueIndex).offer(new TransactionalVNFEvent(0, instanceID, -1, 0, 0, 0, 0, 0));
                        }
                    }
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }

                break;
            }
        }
    }
}
