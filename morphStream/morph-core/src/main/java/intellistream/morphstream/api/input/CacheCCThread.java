package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CacheCCThread implements Runnable {
    private final BlockingQueue<byte[]> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;

    public CacheCCThread(BlockingQueue<byte[]> operationQueue) {
        this.operationQueue = operationQueue;
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] txnByteArray = operationQueue.poll();
            int instanceID = 0;
            String tupleID = "0"; //TODO: Hardcoded
            int value = 0;

            for (Map.Entry<Integer, Socket> entry : instanceSocketMap.entrySet()) {
                if (entry.getKey() != instanceID) {
                    try {
                        OutputStream out = instanceSocketMap.get(entry.getKey()).getOutputStream();
                        String combined = 3 + ";" + tupleID + ";" + value; //__update_instance_cache
                        byte[] byteArray = combined.getBytes();
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
