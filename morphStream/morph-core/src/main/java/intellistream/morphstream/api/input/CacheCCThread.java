package intellistream.morphstream.api.input;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.simVNF.VNFInstance;
import intellistream.morphstream.api.input.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CacheCCThread implements Runnable {
    private static BlockingQueue<CacheData> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");

    public CacheCCThread(BlockingQueue<CacheData> operationQueue) {
        CacheCCThread.operationQueue = operationQueue;
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    }

    public static void submitReplicationRequest(CacheData cacheData) {
        try {
            operationQueue.put(cacheData);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        if (communicationChoice == 0) { // Java sim VNF
            while (!Thread.currentThread().isInterrupted()) {
                CacheData cacheData;
                try {
                    cacheData = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (cacheData.getTimestamp() == -1) {
                    System.out.println("Cache CC thread received stop signal");
                    break;
                }

                // Simulating state update synchronization to other instances
                for (Map.Entry<Integer, VNFInstance> entry : VNFRunner.getSenderMap().entrySet()) {
                    if (entry.getKey() != cacheData.getInstanceID()) {
                        SyncData syncData = new SyncData(cacheData.getTimestamp(), cacheData.getTupleID(), cacheData.getValue());
                        entry.getValue().submitSyncData(syncData);
                    }
                }

                try {
                    VNFRequest request = new VNFRequest((int) cacheData.getTxnReqID(), cacheData.getInstanceID(), cacheData.getTupleID(), 0, cacheData.getTimestamp());
                    VNFRunner.getSender(cacheData.getInstanceID()).submitFinishedRequest(request);
                } catch (NullPointerException e) {
                    throw new RuntimeException(e);
                }

            }

        } else if (communicationChoice == 1) {
            while (!Thread.currentThread().isInterrupted()) {
                CacheData cacheData;
                try {
                    cacheData = operationQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (cacheData.getTimestamp() == -1) {
                    System.out.println("Cache CC thread received stop signal");
                    break;
                }
                int tupleID = cacheData.getTupleID();
                int value = cacheData.getValue();

                for (Map.Entry<Integer, Socket> entry : instanceSocketMap.entrySet()) {
                    if (entry.getKey() != cacheData.getInstanceID()) {
                        try {
                            AdaptiveCCManager.vnfStubs.get(cacheData.getInstanceID()).update_value(tupleID, value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

}
