package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CacheCCThread implements Runnable {
    private final BlockingQueue<byte[]> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;

    public CacheCCThread(BlockingQueue<byte[]> operationQueue) {
        this.operationQueue = operationQueue;
        this.instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    }

// instanceID(int) -0
// target = 2 (int) -1
//tupleID(int) -2
//Content(int) -3

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] txnByteArray;
            try {
                txnByteArray = operationQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            List<byte[]> splitByteArrays = splitByteArray(txnByteArray, fullSeparator);
            int instanceID = decodeInt(splitByteArrays.get(0), 0);
            int tupleID = decodeInt(splitByteArrays.get(2), 0);
            int value = decodeInt(splitByteArrays.get(3), 0);

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

    private static long decodeLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (bytes[offset + i] & 0xFF)) << (i * 8);
        }
        return value;
    }

    private static int decodeInt(byte[] bytes, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= (bytes[offset + i] & 0xFF) << (i * 8);
        }
        return value;
    }

    private static List<byte[]> splitByteArray(byte[] byteArray, byte separator) {
        List<byte[]> splitByteArrays = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();

        for (int i = 0; i < byteArray.length; i++) {
            if (byteArray[i] == separator) {
                indexes.add(i);
            }
        }

        int startIndex = 0;
        for (Integer index : indexes) {
            byte[] subArray = new byte[index - startIndex];
            System.arraycopy(byteArray, startIndex, subArray, 0, index - startIndex);
            splitByteArrays.add(subArray);
            startIndex = index + 1;
        }

        // Handling the remaining part after the last occurrence of 59
        if (startIndex < byteArray.length) {
            byte[] subArray = new byte[byteArray.length - startIndex];
            System.arraycopy(byteArray, startIndex, subArray, 0, byteArray.length - startIndex);
            splitByteArrays.add(subArray);
        }

        return splitByteArrays;
    }

}
