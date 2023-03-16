package benchmark.datagenerator.apps.EDS.TPGTxnGenerator;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.EDS.TPGTxnGenerator.Transaction.EDSTREvent;
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

public class EDSTPGDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(EDSTPGDataGenerator.class);
    private final int Transaction_Length; // transaction length, 4 or 8 or longer
    // independent transactions.
    private final boolean isUnique = false;

    private int floor_interval;


    private final Random random = new Random(0); // the transaction type decider
    public transient FastZipfGenerator p_generator; // partition generator
    private final HashMap<Integer, Integer> nGeneratedAccountIds = new HashMap<>();
    private final HashMap<Integer, Integer> nGeneratedAssetIds = new HashMap<>();
    private ArrayList<Event> events;
    private int eventID = 0;
    private final HashMap<Integer, Integer> idToLevel = new HashMap<>();


    public EDSTPGDataGenerator(EDSTPGDataGeneratorConfig dataConfig) {
        super(dataConfig);

        Transaction_Length = dataConfig.Transaction_Length;

        int nKeyState = dataConfig.getnKeyStates();

        // allocate levels for each key, to prevent circular.
//        int MAX_LEVEL = (nKeyState / dataConfig.getTotalThreads()) / 2;
        int MAX_LEVEL = 256;
        for (int i = 0; i < nKeyState; i++) {
            idToLevel.put(i, i% MAX_LEVEL);
        }

        events = new ArrayList<>(nTuples); //total number of input events
    }

    public static void main(String[] args) {
        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(10, 1, 0);
        fastZipfGenerator.show_sample();
    }

    @Override
    protected void generateTuple() {
        EDSTREvent event = randomEvent();
//        System.out.println(eventID);
        events.add(event);
    }

    private EDSTREvent randomEvent() {

        int tweetID = eventID;
        String[] words = {"wordA", "wordB", "wordC"};

        EDSTREvent t = new EDSTREvent(eventID, tweetID, words);

        eventID++;
        return t;
    }

    //Copied from GSW, Method used to store event data into file
    @Override
    public void dumpGeneratedDataToFile() {

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
