package benchmark.datagenerator.apps.ED.TPGTxnGenerator;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.ED.TPGTxnGenerator.Transaction.EDTREvent;
import common.tools.FastZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import static common.CONTROL.enable_log;
import static common.CONTROL.useShortDataset;

/**
 * Read from Stock Dataset, construct events accordingly.
 */
public class TweetDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TweetDataGenerator.class);

    private ArrayList<Event> events;
    private int eventID = 0;

    private final HashMap<Integer, Integer> idToLevel = new HashMap<>();

    private final BufferedReader reader;


    public TweetDataGenerator(EDTPGDataGeneratorConfig dataConfig) throws FileNotFoundException {
        super(dataConfig);
        String rootFilePath = System.getProperty("user.dir");
        int rootFileIndex = rootFilePath.indexOf("MorphStream");
        String cleanRootFilePath = (rootFileIndex != -1) ? rootFilePath.substring(0, rootFileIndex + "MorphStream".length()) : rootFilePath;
        events = new ArrayList<>(nTuples);
        if (useShortDataset) {
            reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(cleanRootFilePath + "/application/src/main/java/benchmark/datagenerator/apps/ED/dataset/dataset_short.csv")));
        } else {
            reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(cleanRootFilePath + "/application/src/main/java/benchmark/datagenerator/apps/ED/dataset/dataset.csv")));
        }
    }

    public static void main(String[] args) {
        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(10, 1, 0);
        fastZipfGenerator.show_sample();
    }


    @Override
    protected void generateTuple() {
        String txn;
        try {
            txn = reader.readLine();
            if (txn != null) {
                String[] split = txn.split(",");
                if (split.length == 3) {
                    String[] words = split[2].split(" ");
                    EDTREvent event = new EDTREvent(Integer.parseInt(split[1]), Integer.parseInt(split[1]), words);
                    events.add(event);
                } else {
                    LOG.info("Invalid event line");
                }
            } else {
                LOG.info("No more events, adding stopping signals...");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
