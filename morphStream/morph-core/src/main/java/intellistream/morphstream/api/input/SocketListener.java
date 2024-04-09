package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.CONTROL;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
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
                    // Convert senderPort to byte array and concatenate with ";" and message
                    // Format: senderPort + ";" + message
                    String senderPortStr = senderPort + ";";
                    byte[] senderPortBytes = senderPortStr.getBytes();
                    byte[] result = new byte[senderPortBytes.length + bytesRead];
                    System.arraycopy(senderPortBytes, 0, result, 0, senderPortBytes.length);
                    System.arraycopy(buffer, 0, result, senderPortBytes.length, bytesRead);

                    System.out.println("Received message from port " + senderPort + ": " + new String(buffer, 0, bytesRead));

                    int target = 0;

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

        int instanceID = 0; //TODO: Hardcoded, should be extracted from the byte array

        byte[] reqIDByte = splitByteArrays.get(1);
        byte[] keysByte = splitByteArrays.get(2);
        byte[] flagByte = splitByteArrays.get(3);
        byte[] isAbortByte = splitByteArrays.get(4);

        long timestamp = 0; // TODO: Pass-in timestamp from VNF instance

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.clear();
        buffer.put(reqIDByte);
        buffer.flip();
        long txnReqID = buffer.getLong();
        assert (txnReqID & 0xfffffff000000000L) == 0 : "Assertion failed: (txnReqId & 0xfffffff000000000) != 0";

        List<byte[]> splitKeyByteArrays = splitByteArray(keysByte, keySeparator);
        String[] keys = new String[splitKeyByteArrays.size()];
        for (int i = 0; i < splitKeyByteArrays.size(); i++) {
            keys[i] = new String(splitKeyByteArrays.get(i), StandardCharsets.US_ASCII);
        }

        int flag = decodeInt(flagByte, 0);
        int isAbort = decodeInt(isAbortByte, 0);
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
