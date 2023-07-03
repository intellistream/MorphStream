package benchmark.datagenerator.apps.TP;

import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.TP.Transaction.TollProcessingEvent;
import benchmark.dynamicWorkloadGenerator.DynamicDataGeneratorConfig;
import benchmark.dynamicWorkloadGenerator.DynamicWorkloadGenerator;
import common.tools.FastZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_states_partition;

/**
 * Dynamic workload generator for toll processing
 * Created by curry on 18/3/22
 */
public class TPTPGDynamicDataGenerator extends DynamicWorkloadGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TPTPGDynamicDataGenerator.class);
    private int State_Access_Skewness; // ratio of state access, following zipf distribution
    private int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    // control the number of txns overlap with each other.
    private ArrayList<Integer> generatedKeys = new ArrayList<>();
    // independent transactions.
    private boolean isUnique = false;
    private FastZipfGenerator keyZipf;

    private int floor_interval;
    public FastZipfGenerator[] partitionedKeyZipf;
    private Random random = new Random(0); // the transaction type decider
    public transient FastZipfGenerator p_generator; // partition generator
    private HashMap<Integer, Integer> nGeneratedIds = new HashMap<>();
    private ArrayList<Event> events;
    private int eventID = 0;
    private boolean enableGroup;
    private HashMap<Integer, Integer> idToLevel = new HashMap<>();
    public TPTPGDynamicDataGenerator(DynamicDataGeneratorConfig dynamicDataConfig) {
        super(dynamicDataConfig);
        events = new ArrayList<>(nTuples);
    }

    @Override
    public void mapToTPGProperties() {

    }

    @Override
    public void switchConfiguration(String type) {
        State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
        Ratio_of_Transaction_Aborts = dynamicDataConfig.Ratio_of_Transaction_Aborts;
        Ratio_of_Overlapped_Keys = dynamicDataConfig.Ratio_of_Overlapped_Keys;
        this.enableGroup=dynamicDataConfig.enableGroup;
        int nKeyState = dynamicDataConfig.getnKeyStates();
        int MAX_LEVEL = 256;
        for (int i = 0; i < nKeyState; i++) {
            idToLevel.put(i, i% MAX_LEVEL);
        }
        keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
        configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
        p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
    }
    public void configure_store(double scale_factor, double theta, int tthread, int numItems) {
        floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / tthread;
        partitionedKeyZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        if (dynamicDataConfig.enableGroup) {
            String b_ls[]=dynamicDataConfig.skewGroup.split(",");
            for (int i = 0; i < dynamicDataConfig.groupNum; i++) {
                for (int j = i * (tthread /dynamicDataConfig.groupNum); j < (i + 1) * (tthread /dynamicDataConfig.groupNum); j++) {
                    partitionedKeyZipf[j] = new FastZipfGenerator((int) (floor_interval * scale_factor), Double.parseDouble(b_ls[i]) / 100 , j * floor_interval, 12345678);
                }
            }
        } else {
            for (int i = 0; i < tthread; i++) {
                partitionedKeyZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 12345678);
            }
        }
    }

    @Override
    protected void generateTuple() {
        Event event;
        event = randomTPEvent();
        this.events.add(event);
    }

    @Override
    public void dumpGeneratedDataToFile() {
        if (enable_log) LOG.info("++++++" + nGeneratedIds.size());

        if (enable_log) LOG.info("Dumping transactions...");
        try {
            dataOutputHandler.sinkEvents(events);
        } catch (IOException e) {
            e.printStackTrace();
        }

        File versionFile = new File(dynamicDataConfig.getRootPath().substring(0, dynamicDataConfig.getRootPath().length() - 1)
                + String.format("_%d.txt", dynamicDataConfig.getTotalEvents()));
        try {
            versionFile.createNewFile();
            FileWriter fileWriter = new FileWriter(versionFile);
            fileWriter.write(String.format("Total number of threads  : %d\n", dynamicDataConfig.getTotalThreads()));
            fileWriter.write(String.format("Total Events      : %d\n", dynamicDataConfig.getTotalEvents()));
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private Event randomTPEvent() {
        int id;
        int partitionId = 0;
        if (!isUnique) {
            if (enable_states_partition) {
                //Must Determine partitionId from EventId
                if (enableGroup) {
                    partitionId = (eventID % dynamicDataConfig.getTotalThreads());
                } else {
                    partitionId = key_to_partition(p_generator.next());
                }
                id = getKey(partitionedKeyZipf[partitionId],partitionId,generatedKeys);
            } else {
                id = getKey(keyZipf,generatedKeys);
            }
        } else {
            id = getUniqueKey(keyZipf,generatedKeys);
        }
        nGeneratedIds.put(id, nGeneratedIds.getOrDefault(id, 0) + 1);
        Event t;
        //TODO: the "isAbort" does not matter, since the operation of Toll processing never abort
        boolean isAbort;
        if (dynamicDataConfig.enableGroup) {
            if (partitionId < dynamicDataConfig.getTotalThreads()/dynamicDataConfig.groupNum){
                isAbort = random.nextInt(10000) < Ratio_of_Transaction_Aborts;
            } else {
                isAbort = random.nextInt(10000) < Ratio_of_Transaction_Aborts + dynamicDataConfig.Ratio_of_Transaction_Aborts_Highest;
            }
        } else {
            isAbort = random.nextInt(10000) < Ratio_of_Transaction_Aborts;
        }
        t = new TollProcessingEvent(eventID,id,isAbort);
        eventID++;
        return t;
    }
    public int key_to_partition(int key) {
        return (int) Math.floor((double) key / floor_interval);
    }

    private int getKey(FastZipfGenerator zipfGenerator, int partitionId, ArrayList<Integer> generatedKeys) {
        int key;
        key = zipfGenerator.next();
        int next = random.nextInt(100);
        if (next < Ratio_of_Overlapped_Keys) { // randomly select a key from existing keyset.
            if (!generatedKeys.isEmpty()) {
                int counter = 0;
                key = generatedKeys.get(random.nextInt(generatedKeys.size()));
                while (key / floor_interval != partitionId) {
                    key = generatedKeys.get(random.nextInt(generatedKeys.size()));
                    counter++;
                    if (counter >= partitionId) {
                        key = zipfGenerator.next();
                        break;
                    }
                }
            }
        }
        return key;
    }

    private int getKey(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
        int srcKey;
        srcKey = zipfGenerator.next();
        int next = random.nextInt(100);
        if (next < Ratio_of_Overlapped_Keys) { // randomly select a key from existing keyset.
            if (!generatedKeys.isEmpty()) {
                srcKey = generatedKeys.get(zipfGenerator.next() % generatedKeys.size());
            }
        }
        return srcKey;
    }

    private int getUniqueKey(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
        int key;
        key = zipfGenerator.next();
        while (generatedKeys.contains(key)) {
            key = zipfGenerator.next();
        }
        generatedKeys.add(key);
        return key;
    }
}
