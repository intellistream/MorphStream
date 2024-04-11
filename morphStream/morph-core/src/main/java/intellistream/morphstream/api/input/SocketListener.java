package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.CONTROL;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SocketListener implements Runnable { //A single thread that listens for incoming messages from other VMs through a single socket
    private final LinkedBlockingQueue<PatternData> monitorQueue;
    private final LinkedBlockingQueue<PartitionData> partitionQueue;
    private final LinkedBlockingQueue<CacheData> cacheQueue;
    private final LinkedBlockingQueue<OffloadData> offloadQueue;
    private static ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues; //round-robin input queues for each executor (combo/bolt)
    private static final int spoutNum = 4; //TODO: Hardcoded
    private int rrIndex = 0;
    private static final boolean createTimestampForEvent = CONTROL.enable_latency_measurement;
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;
    private final ServerSocket stateManagerSocket = MorphStreamEnv.get().stateManagerSocket();

    public SocketListener(LinkedBlockingQueue<PatternData> monitorQueue,
                          LinkedBlockingQueue<PartitionData> partitionQueue,
                          LinkedBlockingQueue<CacheData> cacheQueue,
                          LinkedBlockingQueue<OffloadData> offloadQueue,
                          ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues) {
        this.monitorQueue = monitorQueue;
        this.partitionQueue = partitionQueue;
        this.cacheQueue = cacheQueue;
        this.offloadQueue = offloadQueue;
        SocketListener.tpgQueues = tpgQueues;
    }


    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (Socket instanceSocket = stateManagerSocket.accept()) {
                System.out.println("Client connected: " + instanceSocket.getRemoteSocketAddress());

                // Record the client socket
                int instanceID = instanceSocket.getPort();
                MorphStreamEnv.get().addInstanceSocket(instanceID, instanceSocket);

                // Get the input stream to read data from the client
                InputStream input = instanceSocket.getInputStream();
                byte[] buffer = new byte[100]; // Buffer to hold the read data

                int bytesRead = input.read(buffer);
                byte[] trimBytes = Arrays.copyOf(buffer, bytesRead);

                if (bytesRead > 0) {

                    List<byte[]> splitByteArrays = splitByteArray(trimBytes, fullSeparator);
                    int target = decodeInt(splitByteArrays.get(1), 0);

                    System.out.println("Received message from port " + instanceID + ": " + new String(buffer, 0, bytesRead) + ", Target = " + target);

                    if (target == 0) {
                        monitorQueue.add(byteToPatternData(instanceID, splitByteArrays));
                    } else if (target == 1) {
                        partitionQueue.add(byteToPartitionData(instanceID, splitByteArrays));
                    } else if (target == 2) {
                        cacheQueue.add(byteToCacheData(instanceID, splitByteArrays));
                    } else if (target == 3) {
                        offloadQueue.add(byteToOffloadData(instanceID, splitByteArrays));
                    } else if (target == 4) {
                        tpgQueues.get(rrIndex).add(byteToTPGData(instanceID, splitByteArrays));
                        rrIndex = (rrIndex + 1) % spoutNum;
                    }
                }

            } catch (IOException e) {
                System.out.println("Exception occurred when trying to connect to VM2: " + e.getMessage());
            }
        }
    }

    private static PatternData byteToPatternData(int instanceID, List<byte[]> splitByteArrays) {
        int tupleID = decodeInt(splitByteArrays.get(1), 0);
        boolean value = decodeBoolean(splitByteArrays.get(2), 0);
        return new PatternData(instanceID, tupleID, value);
    }

    private static PartitionData byteToPartitionData(int instanceID, List<byte[]> splitByteArrays) {
        int tupleID = decodeInt(splitByteArrays.get(1), 0);
        int value = decodeInt(splitByteArrays.get(2), 0);
        return new PartitionData(instanceID, tupleID, value);
    }

    private static CacheData byteToCacheData(int instanceID, List<byte[]> splitByteArrays) {
        int tupleID = decodeInt(splitByteArrays.get(1), 0);
        int value = decodeInt(splitByteArrays.get(2), 0);
        return new CacheData(instanceID, tupleID, value);
    }

    private static OffloadData byteToOffloadData(int instanceID, List<byte[]> splitByteArrays) {
        long timestamp = decodeLong(splitByteArrays.get(1), 0);
        long txnReqID = decodeLong(splitByteArrays.get(2), 0);
        int tupleID = decodeInt(splitByteArrays.get(3), 0);
        int txnIndex = decodeInt(splitByteArrays.get(4), 0);
        int saIndex = decodeInt(splitByteArrays.get(5), 0);
        int isAbort = decodeInt(splitByteArrays.get(6), 0);
        return new OffloadData(instanceID, timestamp, txnReqID, tupleID, txnIndex, saIndex, isAbort);
    }

    private static TransactionalEvent byteToTPGData(int instanceID, List<byte[]> splitByteArrays) {
        long timestamp = decodeLong(splitByteArrays.get(1), 0);
        long txnReqID = decodeLong(splitByteArrays.get(2), 0);
        int tupleID = decodeInt(splitByteArrays.get(3), 0);
        int txnIndex = decodeInt(splitByteArrays.get(4), 0);
        int saIndex = decodeInt(splitByteArrays.get(5), 0);
        int isAbort = decodeInt(splitByteArrays.get(6), 0);
        return new TransactionalVNFEvent(instanceID, timestamp, txnReqID, tupleID, txnIndex, saIndex, isAbort);
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

    private static boolean decodeBoolean(byte[] bytes, int offset) {
        return bytes[offset] != 0;
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
