package intellistream.morphstream.api.input;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.CONTROL;
import lombok.Getter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;


/**
 * This class defines the input data source for application.
 * It should support both static (read from file) and streaming (Kafka, HTTP or WebSocket) input data.
 * It maintains a BlockingQueue to manage data insertion (from data source) and retrieval (by Spout).
 */
public class InputSource {
    private static final InputSource ourInstance = new InputSource();
    @Getter
    private InputSourceType inputSourceType; //from file or streaming
//    private static int spoutNum = MorphStreamEnv.get().configuration().getInt("spoutNum");
    @Getter
    private String staticFilePath; //For now, streaming input is also read from here, difference is that streaming convert data to txnEvent in real time.
    //TODO: Add APIs for other streaming sources: Kafka, HTTP, WebSocket, etc
    private final ConcurrentHashMap<Integer,BlockingQueue<TransactionalEvent>> inputQueues; //stores input data fetched from input source
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
    }

    public void insertStopSignal() { //TODO: Modify workload, so that stop signal can be inserted before spout finish reading all events
//        int spoutNum = MorphStreamEnv.get().configuration().getInt("spoutNum");
        int spoutNum = 4;
        for (int i = 0; i < spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = inputQueues.get(i);
            inputQueue.add(new TransactionalEvent(-1, null, null, null, "stop", false));
        }
    }

    /**
     * For InputSource from file, once file path is specified, automatically convert all lines into TransactionalEvents
     */
    public void initialize(String staticFilePath, InputSourceType inputSourceType, int clientNum) throws IOException {
        this.staticFilePath = staticFilePath;
        this.inputSourceType = inputSourceType;
        BufferedReader csvReader = new BufferedReader(new FileReader(this.staticFilePath));
        String input;
        for (int i = 0; i < clientNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            this.inputQueues.put(i, inputQueue);
        }
        int index = 0;
        while ((input = csvReader.readLine()) != null) {
            if (this.inputSourceType == InputSourceType.FILE_STRING)
                inputQueues.get(index).add(inputFromStringToTxnEvent(input));
            else if (this.inputSourceType == InputSourceType.FILE_JSON)
                inputQueues.get(index).add(inputFromJsonToTxnEvent(input));
            index = (index + 1) % clientNum;
        }
    }

    public BlockingQueue<TransactionalEvent> getInputQueue(int clientId) {
        return this.inputQueues.get(clientId);
    }

    public static TransactionalEvent inputFromJsonToTxnEvent(String input) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(input, JsonObject.class);

        int bid = gson.fromJson(jsonObject.get("subJson0"), Integer.class);
        HashMap<String, List<String>> keyMap = gson.fromJson(jsonObject.get("subJson0"), HashMap.class);
        HashMap<String, Object> valueMap = gson.fromJson(jsonObject.get("subJson1"), HashMap.class);
        HashMap<String, String> valueTypeMap = gson.fromJson(jsonObject.get("subJson2"), HashMap.class);
        String flag = gson.fromJson(jsonObject.get("subJson3"), String.class);
        String isAbort = gson.fromJson(jsonObject.get("subJson4"), String.class);

        TransactionalEvent txnEvent;
        if (isAbort.equals("true")) {
            txnEvent = new TransactionalEvent(bid, keyMap, valueMap, valueTypeMap, flag, true);
        } else {
            txnEvent = new TransactionalEvent(bid, keyMap, valueMap, valueTypeMap, flag, false);
        }
        return txnEvent;
    }

    public static TransactionalEvent inputFromStringToTxnEvent(String input) {
        String [] inputArray = input.split(";");
        HashMap<String, List<String>> keyMap = new HashMap<>();
        HashMap<String, Object> valueMap = new HashMap<>();
        HashMap<String, String> valueTypeMap = new HashMap<>();

        int bid = Integer.parseInt(inputArray[0]);
        String [] keyMapPairs = inputArray[1].split(",");
        for (String pair : keyMapPairs) {
            String[] keyMapPair = pair.split(":");
            List<String> keys = new ArrayList<>(Arrays.asList(keyMapPair).subList(1, keyMapPair.length));
            Set<String> duplicates = keys.stream().filter(i -> Collections.frequency(keys, i) > 1).collect(Collectors.toSet());
            if (!duplicates.isEmpty()) {
                System.out.println("Duplicate elements found: " + duplicates);
            }
            keyMap.put(keyMapPair[0], keys);
        }
        String [] valueMapPairs = inputArray[2].split(",");
        if (!valueMapPairs[0].isEmpty()) {
            for (String mapPair : valueMapPairs) {
                String[] valueMapPair = mapPair.split(":");
                valueMap.put(valueMapPair[0], valueMapPair[1]);
            }
        }
        String [] valueTypeMapPairs = inputArray[3].split(",");
        if (!valueTypeMapPairs[0].isEmpty()){
            for (String typeMapPair : valueTypeMapPairs) {
                String[] valueTypeMapPair = typeMapPair.split(":");
                valueTypeMap.put(valueTypeMapPair[0], valueTypeMapPair[1]);
            }
        }

        String flag = inputArray[4];
        String isAbort = inputArray[5];

        TransactionalEvent txnEvent;
        if (isAbort.equals("true")) {
            txnEvent = new TransactionalEvent(bid, keyMap, valueMap, valueTypeMap, flag, true);
        } else {
            txnEvent = new TransactionalEvent(bid, keyMap, valueMap, valueTypeMap, flag, false);
        }
        return txnEvent;
    }
    public static TransactionalEvent inputFromByteToTxnEvent(byte[] bytes) {
        String string = new String(bytes, StandardCharsets.UTF_8);
        return inputFromStringToTxnEvent(string);
    }

}
