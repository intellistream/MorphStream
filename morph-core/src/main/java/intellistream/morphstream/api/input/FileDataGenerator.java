package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.util.FastZipfGenerator;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


/**
 * A data generator defined by client based on customized needs.
 * We are expecting the client to generate data file that matches the format of its TransactionalEvent 
 */
public class FileDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(FileDataGenerator.class);
    private final Random random = new Random(0);
    private Configuration configuration = MorphStreamEnv.get().configuration();
    //File configuration
    private String fileName;
    private String rootPath;
    //System configuration
    private int totalThreads = 4;
    private int punctuation;
    //Event configure
    private HashMap<String, List<String>> eventKeyMap = new HashMap<>();//event -> key list
    private HashMap<String, List<String>> eventValueMap = new HashMap<>();//event -> value list
    //InputStream configuration
    private int totalEvents;
    private HashMap<String, Integer> numItemMaps;//table name (key) to number of items
    private HashMap<String, Integer> intervalMaps = new HashMap<>();//table name (key) to interval
    private HashMap<String, Integer> stateAssessSkewMap = new HashMap<>();//event -> skew access skewness
    private HashMap<String, Integer> eventRatioMap = new HashMap<>();//event -> event ratio
    private ArrayList<String> eventList = new ArrayList<>();//event list
    private String[] eventType;//event list
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
        return rootPath + fileName;
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
        BufferedWriter transferEventBufferedWriter = CreateWriter(fileName);
        for (TransactionalEvent inputEvent : inputEvents) {
            transferEventBufferedWriter.write(inputEvent + "\n");
        }
        transferEventBufferedWriter.close();
    }
    private BufferedWriter CreateWriter(String FileName) throws IOException {
        File file = new File(rootPath + OsUtils.OS_wrapper(FileName));
        if (!file.exists())
            file.createNewFile();
        return Files.newBufferedWriter(Paths.get(file.getPath()));
    }
    private void generateTuple(String eventType) {
        TransactionalEvent inputEvent;
        String[] keys = generateKey(eventType);
        String[] values = generateValue(eventType);
        HashMap<String, String> keyMap = new HashMap<>();
        for (int i = 0; i < keys.length; i++) {
            keyMap.put(eventKeyMap.get(eventType).get(i), keys[i]);
        }
        HashMap<String, Object> valueMap = new HashMap<>();
        HashMap<String, String> valueTypeMap = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            valueMap.put(eventValueMap.get(eventType).get(i), values[i]);
            valueTypeMap.put(eventValueMap.get(eventType).get(i), "int");
        }
        if (random.nextInt(1000) < eventRatioMap.get(eventType)) {
            inputEvent = new TransactionalEvent(eventID, keyMap, valueMap, valueTypeMap, eventType, true);
        } else {
            inputEvent = new TransactionalEvent(eventID, keyMap, valueMap, valueTypeMap, eventType, false);
        }
        inputEvents.add(inputEvent);
        eventID++;
    }

    private String[] generateKey(String eventType) {
        List<String> tableNames = eventKeyMap.get(eventType);
        String[] keys = new String[eventKeyMap.get(eventType).size()];
        int key = zipfGeneratorHashMap.get(eventType).get(tableNames.get(0)).next();
        keys[0] = String.valueOf(key);
        int partition = key_to_partition(tableNames.get(0), key);
        if (random.nextInt(1000) < eventMultiPartitionMap.get(eventType)) {
            for (int i = 1; i < keys.length; i++) {
                int partition1 = random.nextInt(totalThreads);
                while (partition == partition1) partition1 = random.nextInt(totalThreads);
                keys[i] = String.valueOf(partitionZipfGeneratorHashMap.get(eventType).get(tableNames.get(i)).get(partition1).next());
            }
        } else {
            for (int i = 1; i < keys.length; i++) {
                keys[i] = String.valueOf(partitionZipfGeneratorHashMap.get(eventType).get(tableNames.get(i)).get(partition).next());
            }
        }
        return keys;
    }
    private String[] generateValue(String eventType) {
        String[] values = new String[eventValueMap.get(eventType).size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = String.valueOf(random.nextInt(10000));
        }
        return values;
    }

    private void configure_store() {
        rootPath = configuration.getString("rootPath", "/Users/curryzjj/hair-loss/MorphStream/Benchmark/") + OsUtils.OS_wrapper("inputs");
        if (!new File(rootPath).exists()) {
            new File(rootPath).mkdirs();
        }
        fileName = configuration.getString("fileName", "events.txt");
        totalThreads = configuration.getInt("totalThreads", 4);
        punctuation = configuration.getInt("punctuation", 1000);
        totalEvents = configuration.getInt("totalEvents", totalThreads * punctuation);
        phaseType = configuration.getString("workloadType", "default").split(",");
        phase = 0;
        eventType = configuration.getString("eventTypes", "event1,event2").split(",");
        inputEvents = new ArrayList<TransactionalEvent>(totalEvents);
        for (Map.Entry<String, Integer> s : numItemMaps.entrySet()) {
            intervalMaps.put(s.getKey(), s.getValue() / totalThreads);
        }
        for (String event : eventType) {
            eventKeyMap.put(event, Arrays.asList(configuration.getString(event + "_keys", "key1,key2").split(",")));
            eventValueMap.put(event, Arrays.asList(configuration.getString(event + "_values", "v1,v2").split(",")));
            stateAssessSkewMap.put(event, configuration.getInt(event + ".State_Access_Skewness", 0));
            eventRatioMap.put(event, configuration.getInt(event + ".Ratio_of_Event", 50));
            for (int i = 0; i < eventRatioMap.get(event) / 10; i++) {
                eventList.add(event);
            }
            eventAbortMap.put(event, configuration.getInt(event + ".Ratio_of_Transaction_Aborts", 0));
            eventMultiPartitionMap.put(event, configuration.getInt(event + ".Ratio_of_Multi_Partition_Transactions", 0));
            HashMap<String, FastZipfGenerator> zipfHashMap = new HashMap<>();
            HashMap<String, List<FastZipfGenerator>> partitionZipfHashMap = new HashMap<>();
            for (String key: eventKeyMap.get(event)) {
                zipfHashMap.put(key, new FastZipfGenerator(numItemMaps.get(key), (double) stateAssessSkewMap.get(event) / 100, 0,123456789));
                List<FastZipfGenerator> zipfGenerators = new ArrayList<>();
                for (int i = 0; i < totalThreads; i++) {
                    zipfGenerators.add(new FastZipfGenerator(numItemMaps.get(key), (double) stateAssessSkewMap.get(event) / 100, i * intervalMaps.get(key),123456789));
                }
                partitionZipfHashMap.put(key, zipfGenerators);
            }
            zipfGeneratorHashMap.put(event, zipfHashMap);
            partitionZipfGeneratorHashMap.put(event, partitionZipfHashMap);
        }
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
