package intellistream.morphstream.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import intellistream.morphstream.api.input.TransactionalEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * This class defines the input data source for application.
 * It should support both static (read from file) and streaming (Kafka, HTTP or WebSocket) input data.
 * It maintains a BlockingQueue to manage data insertion (from data source) and retrieval (by Spout).
 */
public class InputSource {

    private final String inputSourceType; //from file or streaming
    private String staticFilePath; //For now, streaming input is also read from here, difference is that streaming convert data to txnEvent in real time.
    //TODO: Add APIs for other streaming sources: Kafka, HTTP, WebSocket, etc
    private final BlockingQueue<TransactionalEvent> inputQueue; //stores input data fetched from input source
    private int bid;

    public InputSource(String inputSourceType) {
        this.inputSourceType = inputSourceType;
        this.inputQueue = new LinkedBlockingQueue<>();
        this.bid = 0;
    }

    /**
     * For InputSource from file, once file path is specified, automatically convert all lines into TransactionalEvents
     */
    public void setStaticInputSource(String staticFilePath) throws IOException {
        this.staticFilePath = staticFilePath;
        BufferedReader csvReader = new BufferedReader(new FileReader(this.staticFilePath));
        String input;
        while ((input = csvReader.readLine()) != null) {
            inputQueue.add(inputToTxnEvent(input));
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

    private TransactionalEvent inputToTxnEvent(String input) {
        // Parse JSON from CSV line
        JsonObject nestedHashMapJson = JsonParser.parseString(input).getAsJsonObject();

        // Extract sub-JSON objects as needed
        JsonObject keysJSON = nestedHashMapJson.getAsJsonObject("keys");
        JsonObject valuesJSON = nestedHashMapJson.getAsJsonObject("values");
        JsonObject valueTypesJSON = nestedHashMapJson.getAsJsonObject("valueTypes");
        JsonObject flagsJSON = nestedHashMapJson.getAsJsonObject("flags");

        // Convert JsonObject to java data structures
        HashMap<String, String> keys = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : keysJSON.entrySet()) {
            keys.put(entry.getKey(), entry.getValue().getAsString());
        }

        HashMap<String, String> values = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : valuesJSON.entrySet()) {
            values.put(entry.getKey(), entry.getValue().getAsString());
        }

        HashMap<String, String> valueTypes = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : valueTypesJSON.entrySet()) {
            valueTypes.put(entry.getKey(), entry.getValue().getAsString());
        }

        String flag = flagsJSON.entrySet().stream().toString();

        TransactionalEvent txnEvent = new TransactionalEvent(this.bid, keys, values, valueTypes, flag);
        bid++;
        return txnEvent;
    }

    public String getStaticFilePath() {
        return this.staticFilePath;
    }

    public String getInputSourceType() {
        return inputSourceType;
    }
}
