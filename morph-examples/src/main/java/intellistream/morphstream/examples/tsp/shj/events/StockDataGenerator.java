package intellistream.morphstream.examples.tsp.shj.events;

import intellistream.morphstream.examples.utils.datagen.DataGenerator;
import intellistream.morphstream.api.input.InputEvent;
import intellistream.morphstream.util.FastZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

/**
 * Read from Stock Dataset, construct events accordingly.
 */
public class StockDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(StockDataGenerator.class);
    private final int eventID = 0;
    private final HashMap<Integer, Integer> idToLevel = new HashMap<>();
    private final BufferedReader reader;
    private ArrayList<InputEvent> inputEvents;


    public StockDataGenerator(SHJTPGDataGeneratorConfig dataConfig) throws FileNotFoundException {
        super(dataConfig);
        inputEvents = new ArrayList<>(nTuples);
        // TODO: hardcoded stock dataset load path
//        reader = new BufferedReader(new InputStreamReader(
//                new FileInputStream("/home/myc/workspace/MorphStream/application/src/main/java/benchmark/datagenerator/apps/SHJ/dataset/stock_dataset.csv")));
        reader = new BufferedReader(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("stock_dataset.csv")));
    }

    public static void main(String[] args) {
        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(10, 1, 0);
        fastZipfGenerator.show_sample();
    }


    @Override
    protected void generateTuple() {
        String txn = null;
        try {
            txn = reader.readLine();

            if (txn != null) {
                String[] split = txn.split(",");

                int[] lookupKeys = new int[]{Integer.parseInt(split[4])};

                SHJInputEvent event = new SHJInputEvent(Integer.parseInt(split[0]),
                        Integer.parseInt(split[1]), split[2], split[3], lookupKeys);
                inputEvents.add(event);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateTuples() {
        String txn = null;
        try {
            txn = reader.readLine();

            //        int p_bids[] = new int[tthread];
            while (txn != null) {
                String[] split = txn.split(",");

                int[] lookupKeys = new int[]{Integer.parseInt(split[4])};

                SHJInputEvent event = new SHJInputEvent(Integer.parseInt(split[0]),
                        Integer.parseInt(split[1]), split[2], split[3], lookupKeys);
                inputEvents.add(event);
                txn = reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dumpGeneratedDataToFile() {

        if (enable_log) LOG.info("Dumping transactions...");
        try {
            dataOutputHandler.sinkEvents(inputEvents);
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
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clearDataStructures() {
        if (inputEvents != null) {
            inputEvents.clear();
        }
        inputEvents = new ArrayList<>();
        // clear the data structure in super class
        super.clearDataStructures();
    }
}
