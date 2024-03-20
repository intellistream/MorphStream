package intellistream.morphstream.api.input;

import intellistream.morphstream.util.libVNFFrontend.NativeInterface;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class OffloadManager implements Runnable {

    private ConcurrentLinkedDeque<TransactionalEvent> txnQueue;
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;

    public OffloadManager() {
        txnQueue = new ConcurrentLinkedDeque<>();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            TransactionalEvent txnEvent = txnQueue.pollFirst();
            int txnType = 1; //TODO: Hardcode, 0:R, 1:W
            int txnResult = -1;
            if (txnType == 1) {
                writeGlobalStates(0,0);
            } else if (txnType == 0) {
                txnResult = readGlobalStates(0);
            }
            NativeInterface.__txn_finished_results(txnEvent.getTxnRequestID(), txnResult);
        }
    }

    //Called by VNF instances
    public void addOffloadTxn(byte[] byteArray) {
        txnQueue.add(inputFromStringToTxnVNFEvent(byteArray));
    }

    private void writeGlobalStates(long tupleID, int value) {
        // Apply serially to DB according to txn timestamps
    }

    private int readGlobalStates(long tupleID) {
        // Check the latest value from DB
        return 0;
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
