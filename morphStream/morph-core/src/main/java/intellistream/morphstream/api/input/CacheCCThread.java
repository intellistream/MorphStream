package intellistream.morphstream.api.input;

import intellistream.morphstream.api.input.simVNF.VNFSenderThread;
import intellistream.morphstream.api.input.simVNF.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CacheCCThread implements Runnable {
    private static BlockingQueue<CacheData> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);

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
        if (serveRemoteVNF) {
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

                for (Map.Entry<Integer, Socket> entry : instanceSocketMap.entrySet()) {
                    if (entry.getKey() != cacheData.getInstanceID()) {
                        try {
                            ByteBuffer byteBuffer = ByteBuffer.allocate(24);
                            byteBuffer.putInt(3);
                            byteBuffer.putChar(';');
                            byteBuffer.putInt(12);
                            byteBuffer.putChar(';');
                            byteBuffer.putInt(cacheData.getTupleID());
                            byteBuffer.putChar(':');
                            byteBuffer.putInt(cacheData.getValue());
                            byteBuffer.putChar(';');
                            byte[] byteArray = byteBuffer.array();

                            OutputStream out = instanceSocketMap.get(entry.getKey()).getOutputStream();
                            out.write(byteArray);
                            out.flush();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }

        } else {
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
                for (Map.Entry<Integer, VNFSenderThread> entry : VNFManager.getSenderMap().entrySet()) {
                    if (entry.getKey() != cacheData.getInstanceID()) {
                        int updateValue = cacheData.getValue();
                        entry.getValue().writeLocalState(cacheData.getTupleID(), updateValue);
                    }
                }
            }
        }
    }

}
