package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.CONTROL;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SocketListener implements Runnable { //A single thread that listens for incoming messages from other VMs through a single socket
    private final LinkedBlockingQueue<byte[]> monitorQueue;
    private final LinkedBlockingQueue<byte[]> partitionQueue;
    private final LinkedBlockingQueue<byte[]> cacheQueue;
    private final LinkedBlockingQueue<byte[]> offloadQueue;
    private static ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues; //round-robin input queues for each executor (combo/bolt)
    private static final int spoutNum = 4; //TODO: Hardcoded
    private int rrIndex = 0;
    private static final boolean createTimestampForEvent = CONTROL.enable_latency_measurement;
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;
    private final ServerSocket stateManagerSocket = MorphStreamEnv.get().stateManagerSocket();

    public SocketListener(LinkedBlockingQueue<byte[]> monitorQueue,
                          LinkedBlockingQueue<byte[]> partitionQueue,
                          LinkedBlockingQueue<byte[]> cacheQueue,
                          LinkedBlockingQueue<byte[]> offloadQueue,
                          ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues) {
        this.monitorQueue = monitorQueue;
        this.partitionQueue = partitionQueue;
        this.cacheQueue = cacheQueue;
        this.offloadQueue = offloadQueue;
        SocketListener.tpgQueues = tpgQueues;
    }


    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (Socket socket = stateManagerSocket.accept()) {

                int senderPort = socket.getPort();
                InputStream input = socket.getInputStream();
                byte[] buffer = new byte[1024]; // Buffer for reading data
                int bytesRead = input.read(buffer); // Read the message into the buffer

                if (bytesRead > 0) {

                    // Format: senderPort + ";" + message
                    String senderPortStr = senderPort + ";";
                    byte[] senderPortBytes = senderPortStr.getBytes();
                    byte[] result = new byte[senderPortBytes.length + bytesRead];
                    System.arraycopy(senderPortBytes, 0, result, 0, senderPortBytes.length);
                    System.arraycopy(buffer, 0, result, senderPortBytes.length, bytesRead);

                    List<byte[]> splitByteArrays = splitByteArray(result, fullSeparator);
                    int target = decodeInt(splitByteArrays.get(1), 0);

                    System.out.println("Received message from port " + senderPort + ": " + new String(buffer, 0, bytesRead));

                    if (target == 0) {
                        monitorQueue.add(result); // Add the concatenated result to the monitor queue
                    } else if (target == 1) {
                        partitionQueue.add(result); // Add the concatenated result to the partition queue
                    } else if (target == 2) {
                        cacheQueue.add(result); // Add the concatenated result to the cache queue
                    } else if (target == 3) {
                        offloadQueue.add(result); // Add the concatenated result to the offload queue
                    } else if (target == 4) {
                        tpgQueues.get(rrIndex).add(inputFromStringToTxnVNFEvent(result));
                        rrIndex = (rrIndex + 1) % spoutNum;
                    }
                }
            } catch (IOException e) {
                System.out.println("Exception occurred when trying to connect to VM2: " + e.getMessage());
            }
        }
    }

    /**
     * Packet string format (split by ";"):
     * ts (timestamp or bid, increasing by each request); txnReqID; key(s) (split by ":"); flag; isAbort
     * */
    private static TransactionalEvent inputFromStringToTxnVNFEvent(byte[] byteArray) {

        List<byte[]> splitByteArrays = splitByteArray(byteArray, fullSeparator);

//instanceID(int) -0
//target = 4 (int) -1
//timeStamp(long) -2
//txnReqId(long) -3
//tupleID (int) -4
//txnIndex(int) -5
//saIndex(int) -6
//isAbort(int) -7

        byte[] instanceIDByte = splitByteArrays.get(0);
        byte[] timestampByte = splitByteArrays.get(2);
        byte[] reqIDByte = splitByteArrays.get(3);
        byte[] tupleIDByte = splitByteArrays.get(4);
        byte[] txnIndexByte = splitByteArrays.get(5);
        byte[] saIndexByte = splitByteArrays.get(6);
        byte[] isAbortByte = splitByteArrays.get(7);

        int instanceID = decodeInt(instanceIDByte, 0);
        long timestamp = decodeLong(timestampByte, 0);
        long txnReqID = decodeLong(reqIDByte, 0);

        int flag = decodeInt(txnIndexByte, 0);
        int isAbort = decodeInt(isAbortByte, 0);


//        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
//        buffer.clear();
//        buffer.put(reqIDByte);
//        buffer.flip();
//        long txnReqID = buffer.getLong();

        List<byte[]> splitKeyByteArrays = splitByteArray(tupleIDByte, keySeparator);
        String[] keys = new String[splitKeyByteArrays.size()];
        for (int i = 0; i < splitKeyByteArrays.size(); i++) {
            keys[i] = new String(splitKeyByteArrays.get(i), StandardCharsets.US_ASCII);
        }

        String flagStr = String.valueOf(flag);
        boolean isAbortBool = isAbort != 0;

        TransactionalVNFEvent txnEvent = new TransactionalVNFEvent(instanceID, timestamp, txnReqID, keys, flagStr, isAbortBool);

        if (createTimestampForEvent) {
            txnEvent.setOriginTimestamp(System.nanoTime()); //TODO: Remove this, timestamp is created at VNF instances
        } else {
            txnEvent.setOriginTimestamp(0L);
        }
        return txnEvent;
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
