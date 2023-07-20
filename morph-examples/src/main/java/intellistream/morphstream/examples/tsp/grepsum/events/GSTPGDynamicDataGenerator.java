package intellistream.morphstream.examples.tsp.grepsum.events;

import intellistream.morphstream.api.InputEvent;
import intellistream.morphstream.examples.utils.datagen.DynamicDataGeneratorConfig;
import intellistream.morphstream.examples.utils.datagen.DynamicWorkloadGenerator;
import intellistream.morphstream.util.AppConfig;
import intellistream.morphstream.util.FastZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.CONTROL.enable_states_partition;

public class GSTPGDynamicDataGenerator extends DynamicWorkloadGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(GSTPGDynamicDataGenerator.class);
    // control the number of txns overlap with each other.
    private final ArrayList<Integer> generatedKeys = new ArrayList<>();
    // independent transactions.
    private final boolean isUnique = false;
    private final Random random = new Random(0); // the transaction type decider
    private final HashMap<Integer, Integer> nGeneratedIds = new HashMap<>();
    private final ArrayList<InputEvent> inputEvents;
    private final HashMap<Integer, Integer> idToLevel = new HashMap<>();
    public FastZipfGenerator[] partitionedKeyZipf;
    public transient FastZipfGenerator p_generator; // partition generator
    private int NUM_ACCESS; // transaction length, 4 or 8 or longer
    private int State_Access_Skewness; // ratio of state access, following zipf distribution
    private int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    private int Transaction_Length;
    private int Ratio_of_Multiple_State_Access;//ratio of multiple state access per transaction
    private int tthread;
    private int nKeyState;
    private FastZipfGenerator keyZipf;
    private int floor_interval;
    private int eventID = 0;

    public GSTPGDynamicDataGenerator(DynamicDataGeneratorConfig dynamicDataConfig) {
        super(dynamicDataConfig);
        inputEvents = new ArrayList<>(nTuples);
    }

    @Override
    public void mapToTPGProperties() {
        //TD,LD,PD,VDD,R_of_A,isCD,isCC,
        StringBuilder stringBuilder = new StringBuilder();
        //TODO:hard code, function not sure
        double td = Transaction_Length * dynamicDataConfig.getCheckpoint_interval();
        td = td * ((double) Ratio_of_Overlapped_Keys / 100);
        stringBuilder.append(td);
        stringBuilder.append(",");
        double ld = Transaction_Length * dynamicDataConfig.getCheckpoint_interval();
        stringBuilder.append(ld);
        stringBuilder.append(",");
        double pd = Transaction_Length * dynamicDataConfig.getCheckpoint_interval() * ((double) Ratio_of_Overlapped_Keys / 100) * NUM_ACCESS;
        stringBuilder.append(pd);
        stringBuilder.append(",");
        stringBuilder.append((double) State_Access_Skewness / 100);
        stringBuilder.append(",");
        stringBuilder.append((double) Ratio_of_Transaction_Aborts / 10000);
        stringBuilder.append(",");
        if (AppConfig.isCyclic) {
            stringBuilder.append("1,");
        } else {
            stringBuilder.append("0,");
        }
        if (AppConfig.complexity < 40000) {
            stringBuilder.append("0,");
        } else {
            stringBuilder.append("1,");
        }
        stringBuilder.append(eventID + dynamicDataConfig.getShiftRate() * dynamicDataConfig.getCheckpoint_interval() * dynamicDataConfig.getTotalThreads());
        this.tranToDecisionConf.add(stringBuilder.toString());
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

                nKeyState = dynamicDataConfig.getnKeyStates();
                int MAX_LEVEL = 256;
                for (int i = 0; i < nKeyState; i++) {
                    idToLevel.put(i, i % MAX_LEVEL);
                }
                keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
                break;
            case "LD":
                Transaction_Length = dynamicDataConfig.Transaction_Length;
                break;
            case "isCyclic":
                Ratio_of_Transaction_Aborts = dynamicDataConfig.Ratio_of_Transaction_Aborts;
                State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
                keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
                AppConfig.isCyclic = true;
                break;
            case "complexity":
                AppConfig.complexity = 80000;
                break;
            case "unchanging":
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        mapToTPGProperties();
    }

    @Override
    protected void generateTuple() {
        GSInputEvent event;
        event = randomEvent();
        inputEvents.add(event);
    }

    @Override
    public void dumpGeneratedDataToFile() {
        if (enable_log) LOG.info("++++++" + nGeneratedIds.size());

        if (enable_log) LOG.info("Dumping transactions...");
        try {
            dataOutputHandler.sinkEvents(inputEvents);
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

    public void configure_store(double scale_factor, double theta, int tthread, int numItems) {
        floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / tthread;
        partitionedKeyZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        for (int i = 0; i < tthread; i++) {
            partitionedKeyZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 12345678);
        }
    }

    private GSInputEvent randomEvent() {
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

        GSInputEvent t;
        if (random.nextInt(10000) < Ratio_of_Transaction_Aborts) {
            t = new GSInputEvent(eventID, keys, true);
        } else {
            t = new GSInputEvent(eventID, keys, false);
        }
        // increase the timestamp i.e. transaction id
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
