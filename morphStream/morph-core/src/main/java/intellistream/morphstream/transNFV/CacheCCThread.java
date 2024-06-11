package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.transNFV.data.SyncData;
import intellistream.morphstream.transNFV.simVNF.VNFInstance;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CacheCCThread implements Runnable {
    private static BlockingQueue<VNFRequest> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");

    public CacheCCThread(BlockingQueue<VNFRequest> operationQueue) {
        CacheCCThread.operationQueue = operationQueue;
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    }

    public static void submitReplicationRequest(VNFRequest request) {
        try {
            operationQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        if (communicationChoice == 0) { // Java sim VNF
            while (!Thread.currentThread().isInterrupted()) {
                VNFRequest request;
                try {
                    request = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (request.getCreateTime() == -1) {
                    System.out.println("Cache CC thread received stop signal");
                    break;
                }

                // Simulating state update synchronization to other instances
                for (Map.Entry<Integer, VNFInstance> entry : VNFRunner.getSenderMap().entrySet()) {
                    if (entry.getKey() != request.getInstanceID()) {
                        SyncData syncData = new SyncData(request.getCreateTime(), request.getTupleID(), request.getValue());
                        entry.getValue().submitSyncData(syncData);
                    }
                }

                try {
                    VNFRunner.getSender(request.getInstanceID()).submitFinishedRequest(request);
                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                }

            }

        } else if (communicationChoice == 1) {
            while (!Thread.currentThread().isInterrupted()) {
                VNFRequest request;
                try {
                    request = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (request.getCreateTime() == -1) {
                    System.out.println("Cache CC thread received stop signal");
                    break;
                }
                int tupleID = request.getTupleID();
                int value = request.getValue();

                for (Map.Entry<Integer, Socket> entry : instanceSocketMap.entrySet()) {
                    if (entry.getKey() != request.getInstanceID()) {
                        try {
                            AdaptiveCCManager.vnfStubs.get(request.getInstanceID()).update_value(tupleID, value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

}
