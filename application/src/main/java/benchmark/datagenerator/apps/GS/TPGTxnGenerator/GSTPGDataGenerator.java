package benchmark.datagenerator.apps.GS.TPGTxnGenerator;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.GS.TPGTxnGenerator.Transaction.GSEvent;
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

import static common.CONTROL.*;

/**
 * \textbf{Workload Configurations.}
 * We extend SL for workload sensitivity study by tweaking its workload generation for varying dependency characteristics. The default configuration and varying values of parameters are summarized in \tony{Table~\ref{}}.
 * Specifically, we vary the following parameters during workload generation.
 * \begin{enumerate}
 * \item \textbf{Ratio of State Access Types:}
 * We vary the ratio of functional dependencies in the workload by tuning the ratio between transfer (w/ functional dependency) and deposit (w/o functional dependency) requests in the input stream.
 * \item \textbf{State Access Skewness:}
 * To present a more realistic scenario, we model the access distribution as Zipfian skew, where certain states are more likely to be accessed than others. The skewness is controlled by the parameter $\theta$ of the Zipfian distribution. More skewed state access also stands for more temporal dependencies in the workload.
 * \item \textbf{Transaction Length:}
 * We vary the number of operations in the same transaction, which essentially determines the logical dependency depth.
 * \item \textbf{Transaction Aborts:}
 * Transaction will be aborted when balance will become negative. To systematically evaluate the effectiveness of \system in handling transaction aborts, we insert artificial abort in state transactions and vary its ratio in the workload.
 * \end{enumerate}
 */
public class GSTPGDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(GSTPGDataGenerator.class);

    private final int NUM_ACCESS; // transaction length, 4 or 8 or longer
    private final int State_Access_Skewness; // ratio of state access, following zipf distribution
    private final int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private final int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    private final int Ratio_of_Multiple_State_Access;//ratio of multiple state access per transaction
    private final int Transaction_Length;
    // control the number of txns overlap with each other.
    private final ArrayList<Integer> generatedKeys = new ArrayList<>();
    // independent transactions.
    private final boolean isUnique = false;
    private final FastZipfGenerator keyZipf;

    private int floor_interval;
    public FastZipfGenerator[] partitionedKeyZipf;


    private final Random random = new Random(0); // the transaction type decider
    public transient FastZipfGenerator p_generator; // partition generator
    private final HashMap<Integer, Integer> nGeneratedIds = new HashMap<>();
    private ArrayList<Event> events;
    private int eventID = 0;

    private final HashMap<Integer, Integer> idToLevel = new HashMap<>();

    public GSTPGDataGenerator(GSTPGDataGeneratorConfig dataConfig) {
        super(dataConfig);

        State_Access_Skewness = dataConfig.State_Access_Skewness;
        NUM_ACCESS = dataConfig.NUM_ACCESS;
        Ratio_of_Transaction_Aborts = dataConfig.Ratio_of_Transaction_Aborts;
        Ratio_of_Overlapped_Keys = dataConfig.Ratio_of_Overlapped_Keys;
        Transaction_Length = dataConfig.Transaction_Length;
        Ratio_of_Multiple_State_Access = dataConfig.Ratio_of_Multiple_State_Access;

        int nKeyState = dataConfig.getnKeyStates();

        // allocate levels for each key, to prevent circular.
//        int MAX_LEVEL = (nKeyState / dataConfig.getTotalThreads()) / 2;
        int MAX_LEVEL = 256;
        for (int i = 0; i < nKeyState; i++) {
            idToLevel.put(i, i% MAX_LEVEL);
        }

        events = new ArrayList<>(nTuples);
        // zipf state access generator
        keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
        configure_store(1, (double) State_Access_Skewness / 100, dataConfig.getTotalThreads(), nKeyState);
        p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
    }

    public static void main(String[] args) {
        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(10, 1, 0);
        fastZipfGenerator.show_sample();
    }

    public void configure_store(double scale_factor, double theta, int tthread, int numItems) {
        floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / tthread;
        partitionedKeyZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        for (int i = 0; i < tthread; i++) {
            partitionedKeyZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 12345678);
        }
    }

    protected void generateTuple() {
        GSEvent event;
        event = randomEvent();
//        System.out.println(eventID);
        events.add(event);
    }

    private GSEvent randomEvent() {
        int NUM_ACCESS;
        if (random.nextInt(100) < Ratio_of_Multiple_State_Access) {
            NUM_ACCESS = this.NUM_ACCESS;
        } else {
            NUM_ACCESS = 1;
        }
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
                        partitionId = (partitionId + 1) % dataConfig.getTotalThreads();
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
        // just for stats record
        for (int key : keys) {
            nGeneratedIds.put(key, nGeneratedIds.getOrDefault(key, 0) + 1);
        }

        GSEvent t;
        if (random.nextInt(10000) < Ratio_of_Transaction_Aborts) {
            t = new GSEvent(eventID, keys, true);
        } else {
            t = new GSEvent(eventID, keys, false);
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

    public void dumpGeneratedDataToFile() {
        if (enable_log) LOG.info("++++++" + nGeneratedIds.size());

        if (enable_log) LOG.info("Dumping transactions...");
        try {
            dataOutputHandler.sinkEvents(events);
        } catch (IOException e) {
            e.printStackTrace();
        }

        File versionFile = new File(dataConfig.getRootPath().substring(0, dataConfig.getRootPath().length() - 1)
                + String.format("_%d.txt", dataConfig.getTotalEvents()));
        try {
            versionFile.createNewFile();
            FileWriter fileWriter = new FileWriter(versionFile);
            fileWriter.write(String.format("Total number of threads  : %d\n", dataConfig.getTotalThreads()));
            fileWriter.write(String.format("Total Events      : %d\n", dataConfig.getTotalEvents()));
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clearDataStructures() {
        if (events != null) {
            events.clear();
        }
        events = new ArrayList<>();
        // clear the data structure in super class
        super.clearDataStructures();
    }
}
