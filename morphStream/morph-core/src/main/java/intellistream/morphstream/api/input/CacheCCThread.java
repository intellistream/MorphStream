package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.nio.ByteBuffer;

public class CacheCCThread implements Runnable {
    private final BlockingQueue<CacheData> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;

    public CacheCCThread(BlockingQueue<CacheData> operationQueue) {
        this.operationQueue = operationQueue;
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            CacheData cacheData;
            try {
                cacheData = operationQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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
    }

}
