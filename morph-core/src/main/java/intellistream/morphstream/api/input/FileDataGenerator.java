package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.util.FastZipfGenerator;
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
    private int totalThreads = 4;
    private int punctuation;
    //Event configure
    private HashMap<String, HashMap<String, Integer>> eventKeyMap = new HashMap<>();//event -> tableName -> keyNumber
    private HashMap<String, List<String>> eventValueNamesMap = new HashMap<>();//event -> value name list
    //InputStream configuration
    private int totalEvents;
    private HashMap<String, Integer> numItemMaps;//table name (key) to number of items
    private HashMap<String, Integer> intervalMaps = new HashMap<>();//table name (key) to interval
    private HashMap<String, Integer> stateAssessSkewMap = new HashMap<>();//event -> skew access skewness
    private HashMap<String, Integer> eventRatioMap = new HashMap<>();//event -> event ratio
    private ArrayList<String> eventList = new ArrayList<>();//event list
    private String[] eventTypes;//event list
    private int eventID = 0;
    private HashMap<String, HashMap<String,FastZipfGenerator>> zipfGeneratorHashMap= new HashMap<>();//event -> key -> zipf generator
    private HashMap<String, HashMap<String, List<FastZipfGenerator>>> partitionZipfGeneratorHashMap = new HashMap<>();//event -> key -> zipf generator
    private HashMap<String, Integer> eventAbortMap = new HashMap<>();//event -> AbortRatio
    private HashMap<String, Integer> eventMultiPartitionMap = new HashMap<>();//event -> MultiPartitionRatio
    private String[] phaseType;//Phase Type for dynamic workload
    protected List<String> tranToDecisionConf = new ArrayList<>();
    private int phase;
    private static ArrayList<TransactionalEvent> inputEvents;
    public String prepareInputData() throws IOException {
        configure_store();
        generateStream();
        dumpGeneratedDataToFile();
        return inputFilePath;
    }
    private void configure_store() {
        configuration = MorphStreamEnv.get().configuration();
        numItemMaps = MorphStreamEnv.get().databaseInitializer().getNumItemMaps();
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
        totalThreads = configuration.getInt("totalThreads", 4);
        punctuation = configuration.getInt("punctuation", 1000);
        totalEvents = configuration.getInt("totalEvents", totalThreads * punctuation);
        phaseType = configuration.getString("workloadType", "default").split(",");
        phase = 0;
        eventTypes = configuration.getString("eventTypes", "event1,event2").split(";");
        inputEvents = new ArrayList<TransactionalEvent>(totalEvents);
        for (Map.Entry<String, Integer> s : numItemMaps.entrySet()) {
            intervalMaps.put(s.getKey(), s.getValue() / totalThreads);
        }
        for (String eventType : eventTypes) {
            String[] tableNames = configuration.getString(eventType + "_tables", "table1,table2").split(",");
            HashMap<String, Integer> keyMap = new HashMap<>();
            String[] keyNumbers = configuration.getString(eventType + "_key_number", "2,2").split(",");
            for (int i = 0; i < tableNames.length; i++) {
                keyMap.put(tableNames[i], Integer.parseInt(keyNumbers[i]));
            }
            eventKeyMap.put(eventType, keyMap);
            eventValueNamesMap.put(eventType, Arrays.asList(configuration.getString(eventType + "_values", "v1,v2").split(",")));
            stateAssessSkewMap.put(eventType, configuration.getInt(eventType + "_state_access_skewness", 0));
            eventRatioMap.put(eventType, configuration.getInt(eventType + "_event_ratio", 50));
            for (int i = 0; i < eventRatioMap.get(eventType) / 10; i++) {
                eventList.add(eventType);
            }
            eventAbortMap.put(eventType, configuration.getInt(eventType + "_abort_ratio", 0));
            eventMultiPartitionMap.put(eventType, configuration.getInt(eventType + "_ratio_of_multi_partition_transactions", 0));
            HashMap<String, FastZipfGenerator> zipfHashMap = new HashMap<>();//tableNames -> zipf generator
            HashMap<String, List<FastZipfGenerator>> partitionZipfHashMap = new HashMap<>();//tableNames -> Lists of partition zipf generator
            for (String tableName: eventKeyMap.get(eventType).keySet()) {
                zipfHashMap.put(tableName, new FastZipfGenerator(numItemMaps.get(tableName), (double) stateAssessSkewMap.get(eventType) / 100, 0,123456789));
                List<FastZipfGenerator> zipfGenerators = new ArrayList<>();
                for (int i = 0; i < totalThreads; i++) {
                    zipfGenerators.add(new FastZipfGenerator(numItemMaps.get(tableName)/totalThreads, (double) stateAssessSkewMap.get(eventType) / 100, i * intervalMaps.get(tableName),123456789));
                }
                partitionZipfHashMap.put(tableName, zipfGenerators);
            }
            zipfGeneratorHashMap.put(eventType, zipfHashMap);
            partitionZipfGeneratorHashMap.put(eventType, partitionZipfHashMap);
        }
    }

    private void generateStream() {
        for (int tupleNumber = 0; tupleNumber < totalEvents + totalThreads; tupleNumber++) {
            if (tupleNumber % (punctuation * totalThreads) == 0) {
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
        for (Map.Entry<String, Integer> entry : keysForTable.entrySet()) {
            List<String> keys = new ArrayList<>();
            int key = zipfGeneratorHashMap.get(eventType).get(entry.getKey()).next();
            keys.add(String.valueOf(key));
            int partition = key_to_partition(entry.getKey(), key);
            if (random.nextInt(1000) < eventMultiPartitionMap.get(eventType)) {
                for (int i = 1; i < entry.getValue(); i++) {
                    int partition1 = random.nextInt(totalThreads);
                    while (partition == partition1) partition1 = random.nextInt(totalThreads);
                    keys.add(String.valueOf(partitionZipfGeneratorHashMap.get(eventType).get(entry.getKey()).get(partition1).next()));
                }
            } else {
                for (int i = 1; i < entry.getValue(); i++) {
                    keys.add(String.valueOf(partitionZipfGeneratorHashMap.get(eventType).get(entry.getKey()).get(partition).next()));
                }
            }
            keyMap.put(entry.getKey(), keys);
        }
        return keyMap;
    }
    private String[] generateValue(String eventType) {
        String[] values = new String[eventValueNamesMap.get(eventType).size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = String.valueOf(random.nextInt(10000));
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
                    for (String event : eventList) {
                        if (stateAssessSkewMap.get(event) + 20 <= 100) {
                            stateAssessSkewMap.put(event, stateAssessSkewMap.get(event) + 20);
                        }
                    }
                    break;
                case "Down_skew":
                    for (String event : eventList) {
                        if (stateAssessSkewMap.get(event) - 20 >= 0) {
                            stateAssessSkewMap.put(event, stateAssessSkewMap.get(event) - 20);
                        }
                    }
                    break;
                case "Up_Multi_Partition":
                    for (String event : eventList) {
                        if (eventMultiPartitionMap.get(event) + 20 <= 100) {
                            eventMultiPartitionMap.put(event, eventMultiPartitionMap.get(event) + 20);
                        }
                    }
                    break;
                case "Down_Multi_Partition":
                    for (String event : eventList) {
                        if (eventMultiPartitionMap.get(event) - 20 >= 0) {
                            eventMultiPartitionMap.put(event, eventMultiPartitionMap.get(event) - 20);
                        }
                    }
                    break;
                case "Up_abort":
                    for (String event : eventList) {
                        if (eventAbortMap.get(event) + 20 <= 100) {
                            eventAbortMap.put(event, eventAbortMap.get(event) + 20);
                        }
                    }
                    break;
                case "Down_abort":
                    for (String event : eventList) {
                        if (eventAbortMap.get(event) - 20 >= 0) {
                            eventAbortMap.put(event, eventAbortMap.get(event) - 20);
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
        this.tranToDecisionConf.add(stringBuilder.toString());
    }
    
}
