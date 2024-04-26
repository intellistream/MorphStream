package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class PartitionCCThread implements Runnable {
    private static BlockingQueue<PartitionData> operationQueue;
    private final Map<Integer, Socket> instanceSocketMap;
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;
    private static HashMap<Integer, Integer> partitionOwnership = MorphStreamEnv.get().stateInstanceMap(); //Maps each state partition to its current owner VNF instance.
    //TODO: The key should labels partition start index as optimization

    public PartitionCCThread(BlockingQueue<PartitionData> operationQueue) {
        PartitionCCThread.operationQueue = operationQueue;
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
        while (!Thread.currentThread().isInterrupted()) {
            PartitionData partitionData;
            try {
                partitionData = operationQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            int targetInstanceID = partitionOwnership.get(partitionData.getTupleID());
//            int targetInstanceID = partitionOwnership.get(partitionData.getTupleID() % partitionOwnership.size()); TODO: Consider this

            try {
                OutputStream out = instanceSocketMap.get(targetInstanceID).getOutputStream(); //TODO: Current workloads do not require cross-partition state access
                String combined =  4 + ";" + partitionData.getValue(); //__txn_finished
                byte[] byteArray = combined.getBytes();
                out.write(byteArray);
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
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
