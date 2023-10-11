package intellistream.morphstream.api.input;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
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
    private static int spoutNum = MorphStreamEnv.get().configuration().getInt("spoutNum");
    private String staticFilePath; //For now, streaming input is also read from here, difference is that streaming convert data to txnEvent in real time.
    //TODO: Add APIs for other streaming sources: Kafka, HTTP, WebSocket, etc
    private final ConcurrentHashMap<Integer,BlockingQueue<TransactionalEvent>> inputQueues; //stores input data fetched from input source
    private int bid;
    private boolean createTimestampForEvent = CONTROL.enable_latency_measurement;
    public enum InputSourceType {
        FILE_STRING,
        FILE_JSON,
        KAFKA,
        HTTP,
        WEBSOCKET
    }

    public static InputSource get() {
        return ourInstance;
    }

    public InputSource() {
        this.inputQueues = new ConcurrentHashMap<>();
        this.bid = 0;
    }

    public void insertStopSignal() { //TODO: Modify workload, so that stop signal can be inserted before spout finish reading all events
        for (int i = 0; i < spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = inputQueues.get(i);
            inputQueue.add(new TransactionalEvent(-1, null, null, null, "stop", false));
        }
    }

    /**
     * For InputSource from file, once file path is specified, automatically convert all lines into TransactionalEvents
     */
    public void initialize(String staticFilePath, InputSourceType inputSourceType) throws IOException {

        this.staticFilePath = staticFilePath;
        this.inputSourceType = inputSourceType;
        BufferedReader csvReader = new BufferedReader(new FileReader(this.staticFilePath));
        String input;
        for (int i = 0; i < spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            this.inputQueues.put(i, inputQueue);
        }
        int index = 0;
        while ((input = csvReader.readLine()) != null) {
            if (this.inputSourceType == InputSourceType.FILE_STRING)
                inputQueues.get(index).add(inputFromStringToTxnEvent(input));
            else if (this.inputSourceType == InputSourceType.FILE_JSON)
                inputQueues.get(index).add(inputFromJsonToTxnEvent(input));
            index = (index + 1) % spoutNum;
        }
    }

    public BlockingQueue<TransactionalEvent> getInputQueue(int spoutId) {
        return this.inputQueues.get(spoutId);
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
