package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.util.AppConfig;
import intellistream.morphstream.util.FastZipfGenerator;
import intellistream.morphstream.util.FixedLengthRandomString;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * A data generator defined by client based on customized needs.
 * We are expecting the client to generate data file that matches the format of its TransactionalEvent 
 */
public class FileDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(FileDataGenerator.class);
    private final Random random = new Random(0);
    private Configuration configuration;
    //File configuration
    private String inputFilePath;
    private String rootPath;
    //System configuration
    private int totalPartition = 4;
    private int punctuation;
    //Event configure
    private HashMap<String, HashMap<String, Integer>> eventKeyMap = new HashMap<>();//event -> tableName -> keyNumber
    private HashMap<String, List<String>> eventValueNamesMap = new HashMap<>();//event -> value name list
    private HashMap<String, List<String>> eventValueLengthMap = new HashMap<>();//event -> value Length
    //InputStream configuration
    private int totalEvents;
    private HashMap<String, Integer> numItemMaps = new HashMap<>();//table name (key) to number of items
    private HashMap<String, Integer> intervalMaps = new HashMap<>();//table name (key) to interval
    private HashMap<String, Double> stateAssessSkewMap = new HashMap<>();//event -> skew access skewness
    private HashMap<String, Integer> eventRatioMap = new HashMap<>();//event -> event ratio
    private ArrayList<String> eventList = new ArrayList<>();//event list
    private String[] eventTypes;//event list
    private int eventID = 0;
    private HashMap<String, HashMap<String, FastZipfGenerator>> zipfGeneratorHashMap= new HashMap<>();//event -> key -> zipf generator
    private HashMap<String, HashMap<String, List<FastZipfGenerator>>> partitionZipfGeneratorHashMap = new HashMap<>();//event -> key -> zipf generator
    private HashMap<String, Double> eventAbortMap = new HashMap<>();//event -> AbortRatio
    private HashMap<String, Integer> eventMultiPartitionMap = new HashMap<>();//event -> MultiPartitionRatio
    private String[] phaseType;//Phase Type for dynamic workload
    @Getter
    protected List<String> tranToDecisionConf = new ArrayList<>();
    private int phase;
    private static ArrayList<TransactionalEvent> inputEvents;
    public String prepareInputData(boolean isExist) throws IOException {
        configure_store();
        if (isExist) {
            generateTPGProperties();
        } else {
            generateStream();
            dumpGeneratedDataToFile();
        }
        return inputFilePath;
    }
    private void configure_store() {
        configuration = MorphStreamEnv.get().configuration();
        String[] tableNames = configuration.getString("tableNames","table1,table2").split(";");
        for (String tableName : tableNames) {
            numItemMaps.put(tableName, configuration.getInt(tableName + "_num_items", 1000000));
        }
        rootPath = configuration.getString("rootPath", "/Users/curryzjj/hair-loss/MorphStream/Benchmark");
        if (!new File(rootPath).exists()) {
            new File(rootPath).mkdirs();
        }
        inputFilePath = configuration.getString("inputFilePath", "events.txt");
        Path inputFile = Paths.get(inputFilePath);
        try {
            if (!Files.exists(inputFile) || !Files.exists(inputFile.getParent())) {
                Files.createDirectories(inputFile.getParent());
                Files.createFile(inputFile);
            }
        } catch (IOException e) {
            System.out.println("Error in locating input file: " + e.getMessage());
        }
        totalPartition = configuration.getInt("tthread", 4) * configuration.getInt("nodeNum", 1);
        punctuation = configuration.getInt("checkpoint", 1000);
        totalEvents = configuration.getInt("totalEvents", totalPartition * punctuation);
        phaseType = configuration.getString("workloadType", "default").split(",");
        phase = 0;
        eventTypes = configuration.getString("eventTypes", "event1;event2").split(";");
        inputEvents = new ArrayList<TransactionalEvent>(totalEvents);
        for (Map.Entry<String, Integer> s : numItemMaps.entrySet()) {
            intervalMaps.put(s.getKey(), s.getValue() / totalPartition);
        }
        for (String eventType : eventTypes) {
            HashMap<String, Integer> keyMap = new HashMap<>();
            String[] keyNumbers = configuration.getString(eventType + "_key_number", "2,2").split(",");
            for (int i = 0; i < tableNames.length; i++) {
                keyMap.put(tableNames[i], Integer.parseInt(keyNumbers[i]));
            }
            eventKeyMap.put(eventType, keyMap);
            eventValueNamesMap.put(eventType, Arrays.asList(configuration.getString(eventType + "_values", "v1,v2").split(",")));
            eventValueLengthMap.put(eventType, Arrays.asList(configuration.getString(eventType + "_value_length", "10,10").split(",")));
            stateAssessSkewMap.put(eventType, configuration.getDouble(eventType + "_state_access_skewness", 0));
            eventRatioMap.put(eventType, configuration.getInt(eventType + "_event_ratio", 50));
            for (int i = 0; i < eventRatioMap.get(eventType) / 10; i++) {
                eventList.add(eventType);
            }
            eventAbortMap.put(eventType, configuration.getDouble(eventType + "_abort_ratio", 0));
            eventMultiPartitionMap.put(eventType, configuration.getInt(eventType + "_ratio_of_multi_partition_transactions", 0));
            HashMap<String, FastZipfGenerator> zipfHashMap = new HashMap<>();//tableNames -> zipf generator
            HashMap<String, List<FastZipfGenerator>> partitionZipfHashMap = new HashMap<>();//tableNames -> Lists of partition zipf generator
            for (String tableName: eventKeyMap.get(eventType).keySet()) {
                zipfHashMap.put(tableName, new FastZipfGenerator(numItemMaps.get(tableName), (double) stateAssessSkewMap.get(eventType) / 100, 0,random.nextInt()));
                List<FastZipfGenerator> zipfGenerators = new ArrayList<>();
                for (int i = 0; i < totalPartition; i++) {
                    zipfGenerators.add(new FastZipfGenerator(numItemMaps.get(tableName)/ totalPartition, (double) stateAssessSkewMap.get(eventType) / 100, i * intervalMaps.get(tableName),123456789));
                }
                partitionZipfHashMap.put(tableName, zipfGenerators);
            }
            zipfGeneratorHashMap.put(eventType, zipfHashMap);
            partitionZipfGeneratorHashMap.put(eventType, partitionZipfHashMap);
        }
    }

    private void generateStream() {
        for (int tupleNumber = 0; tupleNumber < totalEvents; tupleNumber ++) {
            if (tupleNumber % (punctuation * totalPartition) == 0) {
                nextDataGeneratorConfig();
            }
            generateTuple(nextEvent());
        }
    }
    private void dumpGeneratedDataToFile() throws IOException {
        LOG.info("Dumping generated data to file...");
        sinkEvents();
        LOG.info("Dumping generated data to file... Done!");
    }
    private void sinkEvents() throws IOException {
        BufferedWriter transferEventBufferedWriter = Files.newBufferedWriter(Paths.get(inputFilePath));
        for (TransactionalEvent inputEvent : inputEvents) {
            transferEventBufferedWriter.write(inputEvent + "\n");
        }
        transferEventBufferedWriter.close();
    }
    private void generateTuple(String eventType) {
        TransactionalEvent inputEvent;
        HashMap<String, List<String>> keys = generateKey(eventType);
        String[] values = generateValue(eventType);
        HashMap<String, Object> valueMap = new HashMap<>();
        HashMap<String, String> valueTypeMap = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            valueMap.put(eventValueNamesMap.get(eventType).get(i), values[i]);
            valueTypeMap.put(eventValueNamesMap.get(eventType).get(i), "int");
        }
        if (random.nextInt(1000) < eventRatioMap.get(eventType)) {
            inputEvent = new TransactionalEvent(eventID, keys, valueMap, valueTypeMap, eventType, true);
        } else {
            inputEvent = new TransactionalEvent(eventID, keys, valueMap, valueTypeMap, eventType, false);
        }
        inputEvents.add(inputEvent);
        eventID ++;
    }

    private HashMap<String, List<String>> generateKey(String eventType) {//tableName -> List of keys
        HashMap<String, List<String>> keyMap = new HashMap<>();
        HashMap<String, Integer> keysForTable = this.eventKeyMap.get(eventType);
        for (Map.Entry<String, Integer> entry : keysForTable.entrySet()) {//TableName -> keyNumber
            List<String> keys = new ArrayList<>();
            int key = zipfGeneratorHashMap.get(eventType).get(entry.getKey()).next();
            keys.add(String.valueOf(key));
            int partition = key_to_partition(entry.getKey(), key);
            if (random.nextInt(1000) < eventMultiPartitionMap.get(eventType)) {
                for (int i = 1; i < entry.getValue(); i++) {
                    int partition1 = random.nextInt(totalPartition);
                    while (partition == partition1) partition1 = random.nextInt(totalPartition);
                    keys.add(String.valueOf(partitionZipfGeneratorHashMap.get(eventType).get(entry.getKey()).get(partition1).next()));
                }
            } else {
                for (int i = 1; i < entry.getValue(); i++) {
                    int anotherKey = partitionZipfGeneratorHashMap.get(eventType).get(entry.getKey()).get(partition).next();
                    while (keys.contains(String.valueOf(anotherKey))) anotherKey = partitionZipfGeneratorHashMap.get(eventType).get(entry.getKey()).get(partition).next();
                    keys.add(String.valueOf(anotherKey));
                }
            }
            keyMap.put(entry.getKey(), keys);
        }
        return keyMap;
    }
    private String[] generateValue(String eventType) {
        String[] values = new String[eventValueNamesMap.get(eventType).size()];
        for (int i = 0; i < values.length; i++) {
            int length = Integer.parseInt(eventValueLengthMap.get(eventType).get(i));
            values[i] = FixedLengthRandomString.generateRandomFixedLengthString(length);
        }
        return values;
    }

    private String nextEvent() {
        int next = new Random().nextInt(eventList.size());
        return eventList.get(next);
    }
    public int key_to_partition(String tableName, int key) {
        return (int) Math.floor((double) key / intervalMaps.get(tableName));
    }

    /**
     * Generate the configuration based on type
     * @return
     */
    public void nextDataGeneratorConfig() {
        String phaseType;
        if (phase < this.phaseType.length) {
            phaseType = this.phaseType[phase];
            phase++;
            switch (phaseType) {
                case "default":
                case "unchanging":
                    break;
                case "Up_skew":
                    for (String event : eventTypes) {
                        if (stateAssessSkewMap.get(event) + 0.2 <= 1) {
                            stateAssessSkewMap.put(event, stateAssessSkewMap.get(event) + 0.2);
                        }
                    }
                    break;
                case "Down_skew":
                    for (String event : eventTypes) {
                        if (stateAssessSkewMap.get(event) - 0.2 >= 0) {
                            stateAssessSkewMap.put(event, stateAssessSkewMap.get(event) - 0.2);
                        }
                    }
                    break;
                case "Up_Multi_Partition":
                    for (String event : eventTypes) {
                        if (eventMultiPartitionMap.get(event) + 20 <= 100) {
                            eventMultiPartitionMap.put(event, eventMultiPartitionMap.get(event) + 20);
                        }
                    }
                    break;
                case "Down_Multi_Partition":
                    for (String event : eventTypes) {
                        if (eventMultiPartitionMap.get(event) - 20 >= 0) {
                            eventMultiPartitionMap.put(event, eventMultiPartitionMap.get(event) - 20);
                        }
                    }
                    break;
                case "Up_abort":
                    for (String event : eventTypes) {
                        if (eventAbortMap.get(event) + 0.2 <= 1) {
                            eventAbortMap.put(event, eventAbortMap.get(event) + 0.2);
                        }
                    }
                    break;
                case "Down_abort":
                    for (String event : eventTypes) {
                        if (eventAbortMap.get(event) - 0.2 >= 0) {
                            eventAbortMap.put(event, eventAbortMap.get(event) - 0.2);
                        }
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown phase type: " + phaseType);
            }
            mapToTPGProperties();
        }
    }
    public void mapToTPGProperties() {
        //TD,LD,PD,VDD,Skew,R_of_A,isCD,isCC,
        StringBuilder stringBuilder = new StringBuilder();
        //TODO:hard code, function not sure

        // TD = 1.5 * punctuation
        double td = 1.5;
        stringBuilder.append(td);
        stringBuilder.append(",");
        // LD = 0.5 * punctuation
        double ld = 0.5;
        stringBuilder.append(ld);
        stringBuilder.append(",");
        // PD = 0.5 * punctuation
        double pd = 0.5;
        stringBuilder.append(pd);
        stringBuilder.append(",");
        double skewness = 0;
        for (Map.Entry entry : stateAssessSkewMap.entrySet()) {
            skewness += 0.5 * (double) entry.getValue();
        }
        stringBuilder.append(skewness);
        stringBuilder.append(",");
        double abortRatio = 0;
        for (Map.Entry entry : eventAbortMap.entrySet()) {
            abortRatio += 0.5 * (double) entry.getValue();
        }
        stringBuilder.append(abortRatio);
        stringBuilder.append(",");
        if (AppConfig.isCyclic) {
            stringBuilder.append("1,");
        } else {
            stringBuilder.append("0,");
        }
        if (AppConfig.complexity < 40000){
            stringBuilder.append("0,");
        } else {
            stringBuilder.append("1,");
        }

        stringBuilder.append(eventID + MorphStreamEnv.get().configuration().getInt("checkpoint") * MorphStreamEnv.get().configuration().getInt("tthread"));
        this.tranToDecisionConf.add(stringBuilder.toString());
    }

    public void generateTPGProperties() {
        for (int tupleNumber = 0; tupleNumber < totalEvents; tupleNumber++) {
            if (tupleNumber % (punctuation * totalPartition) == 0) {
                nextDataGeneratorConfig();
            }
        }
    }
    //Pad the string to a specified length, filling the insufficient parts with spaces.
    private String padStringToLength(String str, int length) {
        if (str.length() >= length) {
            return str.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(str);
        while (sb.length() < length) {
            sb.append(' ');
        }
        return sb.toString();
    }
}
