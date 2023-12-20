package intellistream.morphstream.api.input;

import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import intellistream.morphstream.configuration.CONTROL;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * This class defines the input data source for application.
 * It should support both static (read from file) and streaming (Kafka, HTTP or WebSocket) input data.
 * It maintains a BlockingQueue to manage data insertion (from data source) and retrieval (by Spout).
 */
public class InputSource {
    private static final InputSource ourInstance = new InputSource();
    private InputSourceType inputSourceType; //from file or streaming
//    private static int spoutNum = MorphStreamEnv.get().configuration().getInt("spoutNum");
    private String staticFilePath; //For now, streaming input is also read from here, difference is that streaming convert data to txnEvent in real time.
    private static ConcurrentHashMap<Integer,BlockingQueue<TransactionalEvent>> executorInputQueues; //round-robin input queues for each executor (combo/bolt)
    private static int bid;
    private static int spoutNum;
    private static int rrIndex = 0; //round-robin index for distributing input data to executor queues
    private static final boolean createTimestampForEvent = CONTROL.enable_latency_measurement;
    private static final byte fullSeparator = 59;
    private static final byte keySeparator = 58;
    public enum InputSourceType {
        FILE_STRING,
        FILE_JSON,
        KAFKA,
        HTTP,
        WEBSOCKET,
        JNI
    }

    public static InputSource get() {
        return ourInstance;
    }

    public void insertStopSignal() {
//        int spoutNum = MorphStreamEnv.get().configuration().getInt("spoutNum");
//        int spoutNum = 4;
//        for (int i = 0; i < spoutNum; i++) {
//            BlockingQueue<TransactionalEvent> inputQueue = executorInputQueues.get(i);
//            inputQueue.add(new TransactionalEvent(-1, null, null, null, "stop", false));
//        }
    }

    // Delegate calling from libVNF for possible type changing.
    public static void libVNFInsertInputData(byte[] input){
        executorInputQueues.get(rrIndex).add(inputFromStringToTxnVNFEvent(input));
        rrIndex = (rrIndex + 1) % spoutNum;
    }

    /**
     * Handles initialization and data insertion for InputSource from static file
     */
    public void initializeStatic(String staticFilePath, InputSourceType inputSourceType, int spoutNum) throws IOException {
        executorInputQueues = new ConcurrentHashMap<>();
        bid = 0;
        this.staticFilePath = staticFilePath;
        this.inputSourceType = inputSourceType;
        InputSource.spoutNum = spoutNum;
        for (int i = 0; i < spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            executorInputQueues.put(i, inputQueue);
        }
        BufferedReader csvReader = new BufferedReader(new FileReader(this.staticFilePath));
        String input;
        int index = 0;
        while ((input = csvReader.readLine()) != null) {
            if (this.inputSourceType == InputSourceType.FILE_STRING)
                executorInputQueues.get(index).add(inputFromStringToTxnEvent(input));
            else if (this.inputSourceType == InputSourceType.FILE_JSON)
                executorInputQueues.get(index).add(inputFromJsonToTxnEvent(input));
            index = (index + 1) % spoutNum;
        }
    }

    public void initializeStreaming(InputSourceType inputSourceType, int _spoutNum) {
        executorInputQueues = new ConcurrentHashMap<>();
        bid = 0;
        ourInstance.inputSourceType = inputSourceType;
        spoutNum = _spoutNum;
        for (int i = 0; i < _spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            executorInputQueues.put(i, inputQueue);
        }
        rrIndex = 0;
    }

    public BlockingQueue<TransactionalEvent> getInputQueue(int spoutId) {
        return executorInputQueues.get(spoutId);
    }

    /**
     * Packet string format (split by ";"):
     * ts (timestamp or bid, increasing by each request); txnReqID; key(s) (split by ":"); flag; isAbort
     * */
    private static TransactionalEvent inputFromStringToTxnVNFEvent(byte[] byteArray) {

        List<byte[]> splitByteArrays = splitByteArray(byteArray, fullSeparator);

        byte[] bidByte = splitByteArrays.get(0);
        byte[] reqIDByte = splitByteArrays.get(1);
        byte[] keysByte = splitByteArrays.get(2);
        byte[] flagByte = splitByteArrays.get(3);
        byte[] isAbortByte = splitByteArrays.get(4);

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.clear();
        buffer.put(bidByte);
        buffer.flip();
        long ts = buffer.getLong();

        buffer.clear();
        buffer.put(reqIDByte);
        buffer.flip();
        long txnReqID = buffer.getLong();

        List<byte[]> splitKeyByteArrays = splitByteArray(keysByte, keySeparator);
        String[] keys = new String[splitKeyByteArrays.size()];
        for (int i = 0; i < splitKeyByteArrays.size(); i++) {
            keys[i] = new String(splitKeyByteArrays.get(i), StandardCharsets.US_ASCII);
        }

        int flag = ByteBuffer.wrap(flagByte).getInt();
        int isAbort = ByteBuffer.wrap(isAbortByte).getInt();

        String flagStr = String.valueOf(flag);
        boolean isAbortBool = isAbort != 0;

        TransactionalVNFEvent txnEvent = new TransactionalVNFEvent(bid, txnReqID, keys, flagStr, isAbortBool);

        if (createTimestampForEvent) {
            txnEvent.setOriginTimestamp(System.nanoTime());
        } else {
            txnEvent.setOriginTimestamp(0L);
        }
        bid++;
        return txnEvent;
    }

    private TransactionalEvent inputFromJsonToTxnEvent(String input) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(input, JsonObject.class);

        HashMap<String, List<String>> keyMap = gson.fromJson(jsonObject.get("subJson0"), HashMap.class);
        HashMap<String, Object> valueMap = gson.fromJson(jsonObject.get("subJson1"), HashMap.class);
        HashMap<String, String> valueTypeMap = gson.fromJson(jsonObject.get("subJson2"), HashMap.class);
        String flag = gson.fromJson(jsonObject.get("subJson3"), String.class);
        String isAbort = gson.fromJson(jsonObject.get("subJson4"), String.class);

        TransactionalEvent txnEvent;
        if (isAbort.equals("true")) {
            txnEvent = new TransactionalEvent(this.bid, keyMap, valueMap, valueTypeMap, flag, true);
        } else {
            txnEvent = new TransactionalEvent(this.bid, keyMap, valueMap, valueTypeMap, flag, false);
        }
        if (createTimestampForEvent) {
            txnEvent.setOriginTimestamp(System.nanoTime());
        } else {
            txnEvent.setOriginTimestamp(0L);
        }

        bid++;
        return txnEvent;
    }

    public TransactionalEvent inputFromStringToTxnEvent(String input) {
        String [] inputArray = input.split(";");
        if (inputArray.length == 5) {
            HashMap<String, List<String>> keyMap = new HashMap<>();
            HashMap<String, Object> valueMap = new HashMap<>();
            HashMap<String, String> valueTypeMap = new HashMap<>();
            String [] keyMapPairs = inputArray[0].split(",");

            for (String pair : keyMapPairs) {
                List<String> keys = new ArrayList<>();
                String[] keyMapPair = pair.split(":");
                for (int j = 1; j < keyMapPair.length; j++) {
                    keys.add(keyMapPair[j]);
                }
                keyMap.put(keyMapPair[0], keys);
            }
            String [] valueMapPairs = inputArray[1].split(",");
            for (String mapPair : valueMapPairs) {
                String[] valueMapPair = mapPair.split(":");
                valueMap.put(valueMapPair[0], valueMapPair[1]);
            }
            String [] valueTypeMapPairs = inputArray[2].split(",");
            for (String typeMapPair : valueTypeMapPairs) {
                String[] valueTypeMapPair = typeMapPair.split(":");
                valueTypeMap.put(valueTypeMapPair[0], valueTypeMapPair[1]);
            }
            String flag = inputArray[3];
            String isAbort = inputArray[4];

            TransactionalEvent txnEvent;
            if (isAbort.equals("true")) {
                txnEvent = new TransactionalEvent(this.bid, keyMap, valueMap, valueTypeMap, flag, true);
            } else {
                txnEvent = new TransactionalEvent(this.bid, keyMap, valueMap, valueTypeMap, flag, false);
            }
            if (createTimestampForEvent) {
                txnEvent.setOriginTimestamp(System.nanoTime());
            } else {
                txnEvent.setOriginTimestamp(0L);
            }
            bid++; //bid only used for normal events, not control signals
            return txnEvent;
        } else if (inputArray.length == 1) { //for control signals
            String message = inputArray[0];
            if (Objects.equals(message, "pause")) {
                return new TransactionalEvent(-1, null, null, null, message, false);
            } else {
                throw new UnsupportedOperationException("Unsupported control signal: " + message);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported input format: " + input);
        }
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

    public String getStaticFilePath() {
        return this.staticFilePath;
    }

    public InputSourceType getInputSourceType() {
        return inputSourceType;
    }
}
