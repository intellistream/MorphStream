package cli;

import com.beust.jcommander.Parameter;
import intellistream.morphstream.api.input.InputEvent;
import intellistream.morphstream.examples.tsp.streamledger.events.SLDepositInputEvent;
import intellistream.morphstream.examples.tsp.streamledger.events.SLTransferInputEvent;
import intellistream.morphstream.examples.tsp.streamledger.events.TPGTxnGenerator.SLTPGDataGeneratorConfig;
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


/**
 * A data generator defined by client based on customized needs.
 * We are expecting the client to generate data file that matches the format of its TransactionalEvent 
 */
public class SLFileDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SLFileDataGenerator.class);
    private static String filePath;
    private int fileSize;
    private static int totalThreads = 4;
    private static int totalEvents = 10000;
    private static int totalRecords = 10000;
    
    private static final int Ratio_Of_Deposit = 25;  // ratio of state access type i.e. deposit or transfer
    private static final int State_Access_Skewness = 20; // ratio of state access, following zipf distribution
    private final int Transaction_Length = 4; // transaction length, 4 or 8 or longer
    private static final int Ratio_of_Transaction_Aborts = 0; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private static final int Ratio_of_Overlapped_Keys = 10; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    // control the number of txns overlap with each other.
    private static final ArrayList<Integer> generatedAcc = new ArrayList<>();
    private static final ArrayList<Integer> generatedAst = new ArrayList<>();
    // independent transactions.
    private static final boolean isUnique = false;
    private static final FastZipfGenerator accountZipf = new FastZipfGenerator(totalRecords, (double) State_Access_Skewness / 100, 0, 12345678);
    private static final FastZipfGenerator assetZipf = new FastZipfGenerator(totalRecords, (double) State_Access_Skewness / 100, 0, 123456789);
    private static final Random random = new Random(0); // the transaction type decider
    private static final HashMap<Integer, Integer> nGeneratedAccountIds = new HashMap<>();
    private static final HashMap<Integer, Integer> nGeneratedAssetIds = new HashMap<>();
    private static final HashMap<Integer, Integer> idToLevel = new HashMap<>();
    public static FastZipfGenerator[] partitionedAccountZipf;
    public static FastZipfGenerator[] partitionedAssetZipf;
    public static transient FastZipfGenerator p_generator = new FastZipfGenerator(totalRecords, (double) State_Access_Skewness / 100, 0); // partition generator
    private static int floor_interval;
    private static ArrayList<InputEvent> inputEvents = new ArrayList<>(totalEvents);
    private static int eventID = 0;

    public static void configure_store(double scale_factor, double theta, int tthread, int numItems) {
        floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / tthread;
        partitionedAccountZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        partitionedAssetZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        for (int i = 0; i < tthread; i++) {
            partitionedAccountZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 12345678);
            partitionedAssetZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 123456789);
        }
    }

    protected static void generateTuple() {
        InputEvent inputEvent;
        int next = random.nextInt(100);
        if (next < Ratio_Of_Deposit) {
            inputEvent = randomDepositEvent();
        } else {
            inputEvent = randomTransferEvent();
        }
        inputEvents.add(inputEvent);
    }

    private static InputEvent randomTransferEvent() {
        // make sure source and destination are different
        int srcAcc, dstAcc, srcAst, dstAst;

        if (!isUnique) {
            if (enable_states_partition) {
                int partitionId = key_to_partition(p_generator.next());
                int[] accKeys = getKeys(partitionedAccountZipf[partitionId],
                        partitionedAccountZipf[(partitionId + 2) % totalThreads],
                        partitionId,
                        (partitionId + 2) % totalThreads,
                        generatedAcc);
                srcAcc = accKeys[0];
                dstAcc = accKeys[1];
                int[] astKeys = getKeys(partitionedAssetZipf[(partitionId + 1) % totalThreads],
                        partitionedAssetZipf[(partitionId + 3) % totalThreads],
                        (partitionId + 1) % totalThreads,
                        (partitionId + 3) % totalThreads,
                        generatedAst);
                srcAst = astKeys[0];
                dstAst = astKeys[1];
                assert srcAcc / floor_interval == partitionId;
                assert srcAst / floor_interval == (partitionId + 1) % totalThreads;
                assert dstAcc / floor_interval == (partitionId + 2) % totalThreads;
                assert dstAst / floor_interval == (partitionId + 3) % totalThreads;
            } else {
                int[] accKeys = getKeys(accountZipf, generatedAcc);
                srcAcc = accKeys[0];
                dstAcc = accKeys[1];
                int[] astKeys = getKeys(assetZipf, generatedAst);
                srcAst = astKeys[0];
                dstAst = astKeys[1];
            }
        } else { // generate unique keys for unique txn processing
            srcAcc = getUniqueKey(accountZipf, generatedAcc);
            dstAcc = getUniqueKey(accountZipf, generatedAcc);
            srcAst = getUniqueKey(assetZipf, generatedAst);
            dstAst = getUniqueKey(assetZipf, generatedAst);
        }

        assert srcAcc != dstAcc;
        assert srcAst != dstAst;

        // just for stats record
        nGeneratedAccountIds.put(srcAcc, nGeneratedAccountIds.getOrDefault((long) srcAcc, 0) + 1);
        nGeneratedAccountIds.put(dstAcc, nGeneratedAccountIds.getOrDefault((long) dstAcc, 0) + 1);
        nGeneratedAssetIds.put(srcAst, nGeneratedAccountIds.getOrDefault((long) srcAst, 0) + 1);
        nGeneratedAssetIds.put(dstAst, nGeneratedAccountIds.getOrDefault((long) dstAst, 0) + 1);
        InputEvent t;
        if (random.nextInt(10000) < Ratio_of_Transaction_Aborts) {
            t = new SLTransferInputEvent(eventID, srcAcc, srcAst, dstAcc, dstAst, 100000000, 100000000);
        } else {
            t = new SLTransferInputEvent(eventID, srcAcc, srcAst, dstAcc, dstAst);
        }

        // increase the timestamp i.e. transaction id
        eventID++;
        return t;
    }

    private static InputEvent randomDepositEvent() {
        int acc;
        int ast;
        if (!isUnique) {
            if (enable_states_partition) {
                int partitionId = key_to_partition(p_generator.next());
                acc = getKey(partitionedAccountZipf[partitionId], partitionId, generatedAcc);
                ast = getKey(partitionedAssetZipf[(partitionId + 1) % totalThreads], (partitionId + 1) % totalThreads, generatedAst);
            } else {
                acc = getKey(accountZipf, generatedAcc);
                ast = getKey(assetZipf, generatedAst);
            }
        } else {
            acc = getUniqueKey(accountZipf, generatedAcc);
            ast = getUniqueKey(assetZipf, generatedAst);
        }

        // just for stats record
        nGeneratedAccountIds.put(acc, nGeneratedAccountIds.getOrDefault((long) acc, 0) + 1);
        nGeneratedAssetIds.put(ast, nGeneratedAccountIds.getOrDefault((long) ast, 0) + 1);

        InputEvent t = new SLDepositInputEvent(eventID, acc, ast);

        // increase the timestamp i.e. transaction id
        eventID++;
        return t;
    }

    public static int key_to_partition(int key) {
        return (int) Math.floor((double) key / floor_interval);
    }

    private static int[] getKeys(FastZipfGenerator zipfGeneratorSrc, FastZipfGenerator zipfGeneratorDst, int partitionSrc, int partitionDst, ArrayList<Integer> generatedKeys) {
        int[] keys = new int[2];
        keys[0] = getKey(zipfGeneratorSrc, partitionSrc, generatedKeys);
        keys[1] = getKey(zipfGeneratorDst, partitionDst, generatedKeys);

        if (AppConfig.isCyclic) { // whether to generate acyclic input stream.
            while (keys[0] == keys[1]) {
                keys[0] = getKey(zipfGeneratorSrc, partitionSrc, generatedKeys);
                keys[1] = getKey(zipfGeneratorDst, partitionDst, generatedKeys);
            }
        } else {
            while (keys[0] == keys[1] || idToLevel.get(keys[0]) >= idToLevel.get(keys[1])) {
                keys[0] = getKey(zipfGeneratorSrc, partitionSrc, generatedKeys);
                keys[1] = getKey(zipfGeneratorDst, partitionDst, generatedKeys);
            }
        }

        generatedKeys.add(keys[0]);
        generatedKeys.add(keys[1]);
        return keys;
    }

    private static int getKey(FastZipfGenerator zipfGenerator, int partitionId, ArrayList<Integer> generatedKeys) {
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

    private static int[] getKeys(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
        int[] keys = new int[2];
        keys[0] = getKey(zipfGenerator, generatedKeys);
        keys[1] = getKey(zipfGenerator, generatedKeys);

        if (AppConfig.isCyclic) {
            while (keys[0] == keys[1]) {
                keys[0] = getKey(zipfGenerator, generatedKeys);
                keys[1] = getKey(zipfGenerator, generatedKeys);
            }
        } else {
            while (keys[0] == keys[1] || idToLevel.get(keys[0]) >= idToLevel.get(keys[1])) {
                keys[0] = getKey(zipfGenerator, generatedKeys);
                keys[1] = getKey(zipfGenerator, generatedKeys);
            }
        }

        generatedKeys.add(keys[0]);
        generatedKeys.add(keys[1]);
        return keys;
    }

    private static int getKey(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
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

    private static int getUniqueKey(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
        int key;
        key = zipfGenerator.next();
        while (generatedKeys.contains(key)) {
            key = zipfGenerator.next();
        }
        generatedKeys.add(key);
        return key;
    }

    public static void dumpGeneratedDataToFile() {
        if (enable_log) LOG.info("++++++" + nGeneratedAccountIds.size());
        if (enable_log) LOG.info("++++++" + nGeneratedAssetIds.size());

        if (enable_log) LOG.info("Dumping transactions...");
//        try {
//            dataOutputHandler.sinkEvents(inputEvents);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        File versionFile = new File(filePath + String.format("_%d.txt", totalEvents));
        try {
            versionFile.createNewFile();
            FileWriter fileWriter = new FileWriter(versionFile);
            fileWriter.write(String.format("Total number of threads  : %d\n", totalThreads));
            fileWriter.write(String.format("Total Events      : %d\n", totalEvents));
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void generateStream() {
        for (int tupleNumber = 0; tupleNumber < totalEvents + totalThreads; tupleNumber++) {//add a padding to avoid non-integral-divided problem.
            generateTuple();
        }
    }

    public static void main(String[] args) {
        int MAX_LEVEL = 256;
        for (int i = 0; i < totalRecords; i++) {
            idToLevel.put(i, random.nextInt(MAX_LEVEL));
        }
        configure_store(1, (double) State_Access_Skewness / 100, totalThreads, totalRecords);
        generateStream();
        dumpGeneratedDataToFile();
        
    }
    
}
