package intellistream.morphstream.api.input;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import intellistream.morphstream.configuration.CONTROL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
    private final ConcurrentHashMap<Integer,BlockingQueue<TransactionalEvent>> executorInputQueues; //round-robin input queues for each executor (combo/bolt)
    private int bid;
    private int spoutNum;
    private int rrIndex = 0; //round-robin index for distributing input data to executor queues
    private boolean createTimestampForEvent = CONTROL.enable_latency_measurement;
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

    public InputSource() {
        this.executorInputQueues = new ConcurrentHashMap<>();
        this.bid = 0;
    }

    public void insertStopSignal() { //TODO: Modify workload, so that stop signal can be inserted before spout finish reading all events
//        int spoutNum = MorphStreamEnv.get().configuration().getInt("spoutNum");
        int spoutNum = 4;
        for (int i = 0; i < spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = executorInputQueues.get(i);
            inputQueue.add(new TransactionalEvent(-1, null, null, null, "stop", false));
        }
    }

    /**
     * Receives input data from streaming source (e.g., JNI) and round-robin inserts data into executor input queues
     */
    public void insertInputData(String input) {
        executorInputQueues.get(rrIndex).add(inputFromStringToTxnVNFEvent(input));
        rrIndex = (rrIndex + 1) % spoutNum;
    }

    /**
     * Handles initialization and data insertion for InputSource from static file
     */
    public void initialize(String staticFilePath, InputSourceType inputSourceType, int spoutNum) throws IOException {
        this.staticFilePath = staticFilePath;
        this.inputSourceType = inputSourceType;
        this.spoutNum = spoutNum;
        for (int i = 0; i < spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            this.executorInputQueues.put(i, inputQueue);
        }
        if (this.inputSourceType == InputSourceType.FILE_STRING || this.inputSourceType == InputSourceType.FILE_JSON) {
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
        } else if (this.inputSourceType == InputSourceType.JNI) {
            rrIndex = 0;
        }
    }

    public BlockingQueue<TransactionalEvent> getInputQueue(int spoutId) {
        return this.executorInputQueues.get(spoutId);
    }

    /**
     * Packet string format (split by ";"):
     * ts (timestamp or bid, increasing by each request); txnReqID; key(s) (split by ":"); flag; isAbort
     * */
    public TransactionalEvent inputFromStringToTxnVNFEvent(String request) {
        String[] inputArray = request.split(";");

        if (inputArray.length == 5) {
            long bid = Long.parseLong(inputArray[0]);
            long txnReqID = Long.parseLong(inputArray[1]);
            String[] keys = inputArray[2].split(":");
            String flag = inputArray[3];
            boolean isAbort = Boolean.parseBoolean(inputArray[4]);

            TransactionalVNFEvent txnEvent = new TransactionalVNFEvent(bid, txnReqID, keys, flag, isAbort);

            if (createTimestampForEvent) {
                txnEvent.setOriginTimestamp(System.nanoTime());
            } else {
                txnEvent.setOriginTimestamp(0L);
            }
            return txnEvent;

        } else {
            throw new UnsupportedOperationException("Unsupported input format: " + request);
        }
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

    public String getStaticFilePath() {
        return this.staticFilePath;
    }

    public InputSourceType getInputSourceType() {
        return inputSourceType;
    }
}
