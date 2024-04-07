package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class PartitionCCThread implements Runnable {
    private final BlockingQueue<byte[]> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance. The key labels partition start index.

    public PartitionCCThread(BlockingQueue<byte[]> operationQueue, HashMap<Integer, Integer> partitionOwnership) {
        this.operationQueue = operationQueue;
        PartitionCCThread.partitionOwnership = partitionOwnership;
        instanceSocketMap = MorphStreamEnv.ourInstance.instanceSocketMap();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] txnByteArray = operationQueue.poll();
            int instanceID = 0;
            int txnReqID = 0;
            int tupleID = 0; //TODO: Hardcoded
            int value = 0;
//            int txnResult = NativeInterface.__request_lock(partitionOwnership.get(0), tupleID, value);

            try {
                OutputStream out = instanceSocketMap.get(instanceID).getOutputStream(); //TODO: Current workloads do not require cross-partition state access
                String combined =  4 + ";" + txnReqID; //__txn_finished
                byte[] byteArray = combined.getBytes();
                out.write(byteArray);
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
