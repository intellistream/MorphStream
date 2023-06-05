package benchmark.datagenerator.apps.NonGS.TPGTxnGenerator;

import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.NonGS.TPGTxnGenerator.Transaction.NonGSEvent;
import benchmark.dynamicWorkloadGenerator.DynamicDataGeneratorConfig;
import benchmark.dynamicWorkloadGenerator.DynamicWorkloadGenerator;
import common.tools.FastZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.AppConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_states_partition;

public class NonGSTPGDynamicDataGenerator extends DynamicWorkloadGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(NonGSTPGDynamicDataGenerator.class);
    public FastZipfGenerator[] partitionedKeyZipf;
    public transient FastZipfGenerator p_generator; // partition generator
    private int NUM_ACCESS; // transaction length, 4 or 8 or longer
    private int State_Access_Skewness; // ratio of state access, following zipf distribution
    private int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    private int Ratio_of_Multiple_State_Access;//ratio of multiple state access per transaction
    private int Ratio_of_Non_Deterministic_State_Access;//ratio of non-deterministic state access

    private int Transaction_Length;
    private int nKeyState;
    // control the number of txns overlap with each other.
    private ArrayList<Integer> generatedKeys = new ArrayList<>();
    // independent transactions.
    private boolean isUnique = false;
    private FastZipfGenerator keyZipf;
    private int floor_interval;
    private Random random = new Random(0); // the transaction type decider
    private HashMap<Integer, Integer> nGeneratedIds = new HashMap<>();
    private ArrayList<Event> events;
    private int eventID = 0;
    private HashMap<Integer, Integer> idToLevel = new HashMap<>();
    public NonGSTPGDynamicDataGenerator(DynamicDataGeneratorConfig dynamicDataGeneratorConfig) {
        super(dynamicDataGeneratorConfig);
        events = new ArrayList<>(nTuples);
    }

    @Override
    public void mapToTPGProperties() {

    }

    @Override
    public void switchConfiguration(String type) {
        switch (type) {
            case "default":
                State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
                NUM_ACCESS = dynamicDataConfig.NUM_ACCESS;
                Ratio_of_Transaction_Aborts = dynamicDataConfig.Ratio_of_Transaction_Aborts;
                Ratio_of_Overlapped_Keys = dynamicDataConfig.Ratio_of_Overlapped_Keys;
                Transaction_Length = dynamicDataConfig.Transaction_Length;
                Ratio_of_Multiple_State_Access = dynamicDataConfig.Ratio_of_Multiple_State_Access;
                Ratio_of_Non_Deterministic_State_Access = dynamicDataConfig.Ratio_of_Non_Deterministic_State_Access;

                nKeyState = dynamicDataConfig.getnKeyStates();
                int MAX_LEVEL = 256;
                for (int i = 0; i < nKeyState; i++) {
                    idToLevel.put(i, i % MAX_LEVEL);
                }
                keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
            break;
            default:
                throw new IllegalArgumentException("Invalid configuration type: " + type);
        }
    }

    @Override
    protected void generateTuple() {
        NonGSEvent event;
        event = randomEvent();
        events.add(event);
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
    private void configure_store(double scale_factor, double theta, int tthread, int numItems) {
        floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / NUM_THREADS;
        partitionedKeyZipf = new FastZipfGenerator[tthread];
        for (int i = 0; i < tthread; i++) {
            partitionedKeyZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 12345678);
        }
    }
    private NonGSEvent randomEvent() {
        int NUM_ACCESS;
        if (random.nextInt(100) < Ratio_of_Multiple_State_Access) {
            NUM_ACCESS = this.NUM_ACCESS;
        } else {
            NUM_ACCESS = 1;
        }
        int[] keys = new int[NUM_ACCESS * Transaction_Length];
        int writeLevel = -1;
        if (!isUnique) {
            if (enable_states_partition) {
                for (int j = 0; j < Transaction_Length; j++) {
                    int partitionId = key_to_partition(p_generator.next());
                    for (int i = 0; i < NUM_ACCESS; i++) {
                        int offset = j * NUM_ACCESS + i;
                        if (AppConfig.isCyclic) {
                            int key = getKey(partitionedKeyZipf[partitionId], partitionId, generatedKeys);
                            if (offset % NUM_ACCESS == 0) {
                                // make sure this one is different with other write key
                                for (int k = 0; k < j; k++) {
                                    while (keys[k * NUM_ACCESS] == key) {
                                        key = getKey(partitionedKeyZipf[partitionId], partitionId, generatedKeys);
                                    }
                                }
                            }
                            keys[offset] = key;
                        } else {
                            // TODO: correct it later
                            keys[offset] = getKey(partitionedKeyZipf[partitionId], partitionId, generatedKeys);
                            if (i == 0) {
                                while (idToLevel.get(keys[offset]) == 0) {
                                    keys[offset] = getKey(partitionedKeyZipf[partitionId], partitionId, generatedKeys);
                                }
                                writeLevel = idToLevel.get(keys[offset]);
                            } else {
                                while (writeLevel <= idToLevel.get(keys[offset])) {
                                    keys[offset] = getKey(partitionedKeyZipf[partitionId], partitionId, generatedKeys);
                                }
                            }
                        }
                        partitionId = (partitionId + 1) % dynamicDataConfig.getTotalThreads();
                    }
                }
            } else {
                for (int i = 0; i < NUM_ACCESS; i++) {
                    if (AppConfig.isCyclic) {
                        keys[i] = getKey(keyZipf, generatedKeys);
                    } else {
                        keys[i] = getKey(keyZipf, generatedKeys);
                        if (i == 0) {
                            while (idToLevel.get(keys[i]) == 0) {
                                keys[i] = getKey(keyZipf, generatedKeys);
                            }
                            writeLevel = idToLevel.get(keys[i]);
                        }
                        while (writeLevel <= idToLevel.get(keys[i])) {
                            keys[i] = getKey(keyZipf, generatedKeys);
                        }
                    }
                }
            }
        } else {
            // TODO: add transaction length logic
            for (int i = 0; i < NUM_ACCESS; i++) {
                keys[i] = getUniqueKey(keyZipf, generatedKeys);
            }
        }
        // just for stats record
        for (int key : keys) {
            nGeneratedIds.put(key, nGeneratedIds.getOrDefault(key, 0) + 1);
        }

        NonGSEvent t;
        boolean isAbort;
        boolean isNonDeterministic;
        if (random.nextInt(10000) < Ratio_of_Transaction_Aborts) {
            isAbort = true;
        } else {
            isAbort = false;
        }
        if (random.nextInt(100) < Ratio_of_Non_Deterministic_State_Access) {
            isNonDeterministic = true;
        } else {
            isNonDeterministic = false;
        }
        t = new NonGSEvent(eventID, keys, isAbort, isNonDeterministic);
        eventID++;
        return t;
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
    private int key_to_partition(int key) {
        return (int) Math.floor((double) key / floor_interval);
    }
}
