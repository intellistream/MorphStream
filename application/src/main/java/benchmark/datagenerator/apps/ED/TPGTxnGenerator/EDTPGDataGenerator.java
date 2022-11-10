package benchmark.datagenerator.apps.ED.TPGTxnGenerator;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.ED.TPGTxnGenerator.Transaction.EDTREvent;
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

public class EDTPGDataGenerator  extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(EDTPGDataGenerator.class);

    private final int NUM_ACCESS; // transaction length, 4 or 8 or longer
    private final int State_Access_Skewness; // ratio of state access, following zipf distribution
    private final int Period_of_Window_Reads; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private final int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    private final int Ratio_of_Multiple_State_Access;//ratio of multiple state access per transaction
    private final int Transaction_Length;
    // control the number of txns overlap with each other.
    private final ArrayList<Integer> generatedKeys = new ArrayList<>();
    // independent transactions.
    private final boolean isUnique = false;
    private final FastZipfGenerator keyZipf;

    private int floor_interval;
    public FastZipfGenerator[] partitionedTweetZipf;


    private final Random random = new Random(0); // the transaction type decider
    public transient FastZipfGenerator p_generator; // partition generator
    private final HashMap<Integer, Integer> nGeneratedIds = new HashMap<>();
    private ArrayList<Event> events;
    private int eventID = 0;

    private final HashMap<Integer, Integer> idToLevel = new HashMap<>();


    public EDTPGDataGenerator(EDTPGDataGeneratorConfig dataConfig) {
        super(dataConfig);

        State_Access_Skewness = dataConfig.State_Access_Skewness;
        NUM_ACCESS = dataConfig.NUM_ACCESS;
        Period_of_Window_Reads = dataConfig.Period_of_Window_Reads;
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
        partitionedTweetZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        for (int i = 0; i < tthread; i++) {
            partitionedTweetZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 12345678);
        }
    }

    protected void generateTuple() {
        EDTREvent event;
        event = randomEvent();
//        System.out.println(eventID);
        events.add(event);
    }

    //TODO: Implement this.
    private EDTREvent randomEvent() {
        int id = 0;
        int tweetID = 0;
        String[] words = {"a", "b", "c"};
        EDTREvent t = new EDTREvent(id, tweetID, words);
        return t;
    }

    //TODO: Copied from GSW, Method used during randomEvent generation
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

    //TODO: Copied from GSW, Method used during randomEvent generation
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

    //TODO: Copied from GSW, Method used during randomEvent generation
    private int getUniqueKey(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
        int key;
        key = zipfGenerator.next();
        while (generatedKeys.contains(key)) {
            key = zipfGenerator.next();
        }
        generatedKeys.add(key);
        return key;
    }

    //TODO: Copied from GSW, Method used to store event data into file
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
