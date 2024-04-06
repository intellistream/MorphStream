package intellistream.morphstream.api.input;

import intellistream.morphstream.util.libVNFFrontend.NativeInterface;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public class PartitionInputProcessor implements Runnable {
    private final BlockingQueue<byte[]> operationQueue;
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance. The key labels partition start index.

    public PartitionInputProcessor(BlockingQueue<byte[]> operationQueue, HashMap<Integer, Integer> partitionOwnership) {
        this.operationQueue = operationQueue;
        PartitionInputProcessor.partitionOwnership = partitionOwnership;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] txnByteArray = operationQueue.poll();
            long txnID = 0;
            String tupleID = "0"; //TODO: Hardcoded
            int value = 0;
            int txnResult = NativeInterface.__request_lock(partitionOwnership.get(0), tupleID, value);
            NativeInterface.__txn_finished(txnID);
        }
    }
}
