package intellistream.morphstream.api.input;

import intellistream.morphstream.util.libVNFFrontend.NativeInterface;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class PartitionCCManager implements Runnable {

    private Thread managerThread;
    private static ConcurrentLinkedDeque<byte[]> txnQueue;
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;

    private static final int partitionSize = 10;
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance. The key labels partition start index.

    public PartitionCCManager() {
        txnQueue = new ConcurrentLinkedDeque<>();
        partitionOwnership = new HashMap<>();
    }

    public void initialize() {
        managerThread = new Thread(this);
        managerThread.start();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] txnByteArray = txnQueue.pollFirst();
            long txnID = 0;
            String tupleID = "0"; //TODO: Hardcoded
            int value = 0;
            int txnResult = NativeInterface.__request_lock(partitionOwnership.get(0), tupleID, value);
            NativeInterface.__txn_finished(txnID);
        }
    }

    //Called by VNF instances
    public static void addPartitionTxn(byte[] byteArray) {
        txnQueue.add(byteArray);
    }


    /**
     * Packet string format (split by ";"):
     * ts (timestamp or bid, increasing by each request); txnReqID; key(s) (split by ":"); flag; isAbort
     * */
    private static TransactionalEvent inputFromStringToTxnVNFEvent(byte[] byteArray) {

        List<byte[]> splitByteArrays = splitByteArray(byteArray, fullSeparator);

        byte[] reqIDByte = splitByteArrays.get(1);
        byte[] keysByte = splitByteArrays.get(2);
        byte[] flagByte = splitByteArrays.get(3);
        byte[] isAbortByte = splitByteArrays.get(4);

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

        TransactionalVNFEvent txnEvent = new TransactionalVNFEvent(-1, txnReqID, keys, flagStr, isAbortBool);

        return txnEvent;
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

    private static int decodeInt(byte[] bytes, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= (bytes[offset + i] & 0xFF) << (i * 8);
        }
        return value;
    }

}
