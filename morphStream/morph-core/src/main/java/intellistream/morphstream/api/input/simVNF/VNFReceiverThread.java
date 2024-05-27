package intellistream.morphstream.api.input.simVNF;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.util.ArrayList;
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
    private long overallStartTime;
    private long overallEndTime;
    private final ArrayList<Long> latencyList = new ArrayList<>(); //TODO: Sort request order based on requestID

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
    public long getOverallStartTime() {
        return overallStartTime;
    }
    public long getOverallEndTime() {
        return overallEndTime;
    }
    public ArrayList<Long> getLatencyList() {
        return latencyList;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;
            try {
                request = requestQueue.take();

                if (actualRequestCount == expRequestCount) { // Stop signal indicated by VNF sender thread
                    overallEndTime = System.nanoTime();
                    overallStartTime = VNFManager.getSenderMap().get(instanceID).getOverallStartTime();
                    long overallDuration = overallEndTime - overallStartTime;
                    System.out.println("VNF receiver " + instanceID + " processed all " + actualRequestCount + " requests with throughput " + (actualRequestCount / (overallDuration / 1E9)) + " events/second");
                    MorphStreamEnv.get().simVNFLatch.countDown();

                    int arrivedIndex = finishBarrier.await();
                    if (arrivedIndex == 0) {
                        System.out.println("All VNF receivers have finished processing requests, sending stop signals...");
                        MonitorThread.submitPatternData(new PatternData(-1, instanceID, 0, false));
                        PartitionCCThread.submitPartitionRequest(new PartitionData(-1, 0, instanceID, 0, 0, -1));
                        CacheCCThread.submitReplicationRequest(new CacheData(-1, 0, instanceID, 0));
                        OffloadCCThread.submitOffloadReq(new OffloadData(-1, instanceID, 0, 0, 0, 0, 0, 0));
                        for (int tpgQueueIndex = 0; tpgQueueIndex < tpgQueues.size(); tpgQueueIndex++) {
                            tpgQueues.get(tpgQueueIndex).offer(new TransactionalVNFEvent(0, instanceID, -1, 0, 0, 0, 0, 0));
                        }
                    }
                    break;

                } else {
                    long requestLatency = System.nanoTime() - request.getCreateTime();
                    latencyList.add(requestLatency);
                    actualRequestCount++;
                    System.out.println("VNF receiver " + instanceID + " processed request " + actualRequestCount);
                }

            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
