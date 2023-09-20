package intellistream.morphstream.api.input;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import intellistream.morphstream.api.input.TransactionalEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * This class defines the input data source for application.
 * It should support both static (read from file) and streaming (Kafka, HTTP or WebSocket) input data.
 * It maintains a BlockingQueue to manage data insertion (from data source) and retrieval (by Spout).
 */
public class InputSource {

    private InputSourceType inputSourceType; //from file or streaming
    private String staticFilePath; //For now, streaming input is also read from here, difference is that streaming convert data to txnEvent in real time.
    //TODO: Add APIs for other streaming sources: Kafka, HTTP, WebSocket, etc
    private final BlockingQueue<TransactionalEvent> inputQueue; //stores input data fetched from input source
    private int bid;
    public enum InputSourceType {
        FILE_STRING,
        FILE_JSON,
        KAFKA,
        HTTP,
        WEBSOCKET
    }

    public InputSource() {
        this.inputQueue = new LinkedBlockingQueue<>();
        this.bid = 0;
    }

    /**
     * For InputSource from file, once file path is specified, automatically convert all lines into TransactionalEvents
     */
    public void initialize(String staticFilePath, InputSourceType type) throws IOException {
        this.staticFilePath = staticFilePath;
        this.inputSourceType = type;
        BufferedReader csvReader = new BufferedReader(new FileReader(this.staticFilePath));
        String input;
        while ((input = csvReader.readLine()) != null) {
            if (this.inputSourceType == InputSourceType.FILE_STRING)
                inputQueue.add(inputFromStringToTxnEvent(input));
            else if (this.inputSourceType == InputSourceType.FILE_JSON)
                inputQueue.add(inputFromJsonToTxnEvent(input));
        }
    }

    public TransactionalEvent getNextTxnEvent() {
        try {
            return inputQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private TransactionalEvent inputFromJsonToTxnEvent(String input) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(input, JsonObject.class);

        HashMap<String, List<String>> keyMap = gson.fromJson(jsonObject.get("subJson0"), HashMap.class);
        HashMap<String, Object> valueMap = gson.fromJson(jsonObject.get("subJson1"), HashMap.class);
        HashMap<String, String> valueTypeMap = gson.fromJson(jsonObject.get("subJson2"), HashMap.class);
        //TODO: Add conditionTypeMap per event, or define both value and condition type statically?
        HashMap<String, String> conditionMap = gson.fromJson(jsonObject.get("subJson3"), HashMap.class);
        String flag = gson.fromJson(jsonObject.get("subJson4"), String.class);
        String isAbort = gson.fromJson(jsonObject.get("subJson5"), String.class);

        TransactionalEvent txnEvent;
        if (isAbort.equals("true")) {
            txnEvent = new TransactionalEvent(this.bid, keyMap, valueMap, valueTypeMap, conditionMap, flag, true);
        } else {
            txnEvent = new TransactionalEvent(this.bid, keyMap, valueMap, valueTypeMap,conditionMap, flag, false);
        }

        bid++;
        return txnEvent;
    }

    public TransactionalEvent inputFromStringToTxnEvent(String input) {
        String [] inputArray = input.split(";");
        HashMap<String, List<String>> keyMap = new HashMap<>();
        HashMap<String, Object> valueMap = new HashMap<>();
        HashMap<String, String> valueTypeMap = new HashMap<>();
        HashMap<String, String> conditionMap = new HashMap<>();
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
        if (inputArray.length == 6) {
            String [] conditionMapPairs = inputArray[5].split(",");
            for (String conditionPairString : conditionMapPairs) {
                String[] conditionPair = conditionPairString.split(":");
                conditionMap.put(conditionPair[0], conditionPair[1]);
            }
        }
        TransactionalEvent txnEvent;
        if (isAbort.equals("true")) {
            txnEvent = new TransactionalEvent(this.bid, keyMap, valueMap, valueTypeMap, conditionMap, flag, true);
        } else {
            txnEvent = new TransactionalEvent(this.bid, keyMap, valueMap, valueTypeMap, conditionMap, flag, false);
        }

        bid++;
        return txnEvent;
    }

    public String getStaticFilePath() {
        return this.staticFilePath;
    }

    public InputSourceType getInputSourceType() {
        return inputSourceType;
    }
}
