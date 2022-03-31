package benchmark.datagenerator.apps.OB;

import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.OB.Transaction.BuyEvent;
import benchmark.datagenerator.apps.OB.Transaction.OBEvent;
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

/**
 * Dynamic data generator for OnlineBidding
 * Created by curry on 17/3/22.
 */
public class OBTPGDynamicDataGenerator extends DynamicWorkloadGenerator {
    private static final Logger LOG= LoggerFactory.getLogger(OBTPGDynamicDataGenerator.class);
    private int NUM_ACCESS; // transaction length, 4 or 8 or longer
    private int State_Access_Skewness; // ratio of state access, following zipf distribution
    private int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    private int Transaction_Length;
    private int tthread;
    private int Ratio_of_Buying;
    private int nKeyState;
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
    private HashMap<Integer, Integer> idToLevel = new HashMap<>();
    public OBTPGDynamicDataGenerator(DynamicDataGeneratorConfig dynamicDataConfig) {
        super(dynamicDataConfig);
        events = new ArrayList<>(nTuples);
    }
    @Override
    protected void generateTuple() {
        Event event;
        int next = random.nextInt(100);
        if(next < Ratio_of_Buying) {
            event = randomBuyingEvent();
        } else {
            next = random.nextInt(100);
            if (next > 50){
                event = randomOBEvent(1);
            } else {
                event = randomOBEvent(0);
            }
        }
        events.add(event);
    }
    @Override
    public void tranToDecisionConf() {
        //TD,LD,PD,VDD,R_of_A,isCD,isCC,
        StringBuilder stringBuilder = new StringBuilder();
        //TODO:hard code, function not sure
        double td = Transaction_Length * dynamicDataConfig.getCheckpoint_interval() * (1 - (double)Ratio_of_Buying/100) +
                ((double)Ratio_of_Buying/100) * 1 * dynamicDataConfig.getCheckpoint_interval();
        td = td *((double) Ratio_of_Overlapped_Keys/100);
        stringBuilder.append(td);
        stringBuilder.append(",");
        double ld = Transaction_Length * dynamicDataConfig.getCheckpoint_interval() * (1 - (double)Ratio_of_Buying/100) +
                ((double)Ratio_of_Buying/100) * 1 * dynamicDataConfig.getCheckpoint_interval();
        stringBuilder.append(ld);
        stringBuilder.append(",");
        double pd = 0;
        stringBuilder.append(pd);
        stringBuilder.append(",");
        stringBuilder.append((double) State_Access_Skewness/100);
        stringBuilder.append(",");
        stringBuilder.append((double) Ratio_of_Transaction_Aborts/10000);
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
        stringBuilder.append(eventID+dynamicDataConfig.getShiftRate()*dynamicDataConfig.getCheckpoint_interval()*dynamicDataConfig.getTotalThreads());
        this.tranToDecisionConf.add(stringBuilder.toString());
    }

    @Override
    public void switchConfiguration(String type) {
        switch (type){
            case "default" :
                State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
                NUM_ACCESS = 1;
                Ratio_of_Transaction_Aborts = dynamicDataConfig.Ratio_of_Transaction_Aborts;
                Ratio_of_Overlapped_Keys = dynamicDataConfig.Ratio_of_Overlapped_Keys;
                Transaction_Length = dynamicDataConfig.Transaction_Length;
                Ratio_of_Buying=dynamicDataConfig.Ratio_Of_Buying;

                nKeyState = dynamicDataConfig.getnKeyStates();
                int MAX_LEVEL = 256;
                for (int i = 0; i < nKeyState; i++) {
                    idToLevel.put(i, i% MAX_LEVEL);
                }
                keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
            break;
            case "skew" :
                State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
                nKeyState = dynamicDataConfig.getnKeyStates();
                keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
            break;
            case "abort" :
                Ratio_of_Transaction_Aborts = dynamicDataConfig.Ratio_of_Transaction_Aborts;
            break;
            case "unchanging" :
            break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        tranToDecisionConf();
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

    public void configure_store(double scale_factor, double theta, int tthread, int numItems) {
        floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / tthread;
        partitionedKeyZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        for (int i = 0; i < tthread; i++) {
            partitionedKeyZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 12345678);
        }
    }

    private Event randomOBEvent(int isAlert) {
        int[] keys = new int[NUM_ACCESS*Transaction_Length];
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
                                    while (keys[k*NUM_ACCESS] == key) {
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
            for (int i = 0; i <NUM_ACCESS; i++) {
                keys[i] = getUniqueKey(keyZipf, generatedKeys);
            }
        }
        for (int key : keys) {
            nGeneratedIds.put(key, nGeneratedIds.getOrDefault(key, 0) + 1);
        }
        OBEvent ob;
        if (random.nextInt(10000) < Ratio_of_Transaction_Aborts) {
            ob = new OBEvent(eventID,keys,true,isAlert);
        } else {
            ob = new OBEvent(eventID,keys,false,isAlert);
        }
        eventID++;
        return ob;
    }
     private Event randomBuyingEvent() {
        int id;
        if (!isUnique) {
            if (enable_states_partition) {
                int partitionId = key_to_partition(p_generator.next());
                id = getKey(partitionedKeyZipf[partitionId],partitionId,generatedKeys);
            } else {
                id = getKey(keyZipf,generatedKeys);
            }
        } else {
            id = getUniqueKey(keyZipf,generatedKeys);
        }
         nGeneratedIds.put(id, nGeneratedIds.getOrDefault(id, 0) + 1);
         Event t;
         if (random.nextInt(10000) < Ratio_of_Transaction_Aborts) {
             t = new BuyEvent(eventID,id,true);
         } else {
             t = new BuyEvent(eventID,id,false);
         }
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
