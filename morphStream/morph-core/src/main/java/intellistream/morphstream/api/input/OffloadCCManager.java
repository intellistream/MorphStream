package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.util.libVNFFrontend.NativeInterface;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class OffloadCCManager implements Runnable {

    private Thread managerThread;
    private static ConcurrentLinkedDeque<byte[]> txnQueue;
    private ExecutorService writeExecutor;
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;
    private static final int writeThreadPoolSize = 4; //TODO: Hardcoded

    public OffloadCCManager() {
        txnQueue = new ConcurrentLinkedDeque<>();
        this.writeExecutor = Executors.newFixedThreadPool(writeThreadPoolSize);
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
            int txnType = 1; //TODO: Hardcode, 0:R, 1:W
            if (txnType == 1) {
                NativeInterface.__txn_finished_results(txnID, 0); //Immediate ACK TODO: Pay attention to the case of txn(R+W)
                writeExecutor.submit(() -> writeGlobalStates(txnID, txnByteArray));

            } else if (txnType == 0) {
                synchronized (this) { //TODO: Consider replace lock with MVCC
                    int txnResult = readGlobalStates(txnID, txnByteArray);
                    NativeInterface.__txn_finished_results(txnID, txnResult); //ACK
                }
            }
        }
    }

    //Called by VNF instances
    public static void addOffloadTxn(byte[] byteArray) {
        txnQueue.add(byteArray);
    }

    private static void writeGlobalStates(long txnID, byte[] byteArray) {
        String tableID = "table";
        String tupleID = "0";
        int value = 0;
        try {
            TableRecord condition_record = storageManager.getTable(tableID).SelectKeyRecord(tupleID);
            SchemaRecord srcRecord = condition_record.content_.readPreValues(txnID);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);
            tempo_record.getValues().get(1).setInt(value);
            condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    private static int readGlobalStates(long txnID, byte[] byteArray) {
        // Check the latest value from DB global store
        int value;
        String tupleID = "0";
        try {
            TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(tupleID);
            value = (int) condition_record.content_.readPreValues(txnID).getValues().get(1).getDouble(); //TODO: read the corresponding version, blocking

        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
        return value;
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
