package intellistream.morphstream.api.input;

import intellistream.morphstream.api.input.java_peer.message.VNFCtrlClient;
import intellistream.morphstream.api.input.simVNF.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class PartitionCCThread implements Runnable {
    private static BlockingQueue<PartitionData> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance.
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);
    //TODO: The key should labels partition start index as optimization

    public PartitionCCThread(BlockingQueue<PartitionData> operationQueue, HashMap<Integer, Integer> partitionOwnership) {
        PartitionCCThread.operationQueue = operationQueue;
        PartitionCCThread.partitionOwnership = partitionOwnership;
        instanceSocketMap = MorphStreamEnv.ourInstance.instanceSocketMap();
    }

    public static void submitPartitionRequest(PartitionData partitionData) {
        try {
            operationQueue.put(partitionData);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {

        if (serveRemoteVNF) {
            while (!Thread.currentThread().isInterrupted()) {
                PartitionData partitionData;
                try {
                    partitionData = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (partitionData.getTimeStamp() == -1) {
                    System.out.println("Partition CC thread received stop signal");
                    break;
                }
                int targetInstanceID = partitionOwnership.get(partitionData.getTupleID());
                int tupleID = partitionData.getTupleID();

                //TODO: Add LOCK-based cross-partition state access here

                //TODO: Check type of request, treat R or W differently?

                VNFCtrlClient.fetch_value(tupleID); //TODO: Align with libVNF. Does libVNF offer state partition mapping?

            }

        } else {
            while (!Thread.currentThread().isInterrupted()) {
                PartitionData partitionData;
                try {
                    partitionData = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (partitionData.getTimeStamp() == -1) {
                    System.out.println("Partition CC thread received stop signal");
                    break;
                }
                int targetInstanceID = partitionOwnership.get(partitionData.getTupleID());

                // Simulating cross-partition state access
                try {
                    int targetPartitionState = VNFManager.getSender(targetInstanceID).readLocalState(partitionData.getTupleID());
                    VNFManager.getSender(targetInstanceID).writeLocalState(partitionData.getTupleID(), targetPartitionState);
                    //TODO: Add locking here
                    partitionData.getSenderResponseQueue().add(1);

                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
