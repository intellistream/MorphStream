package benchmark.datagenerator.apps.SL.TPGTxnGenerator;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.SL.Transaction.SLDepositEvent;
import benchmark.datagenerator.apps.SL.Transaction.SLTransferEvent;
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
public class SLTPGDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SLTPGDataGenerator.class);

    private final int Ratio_Of_Deposit;  // ratio of state access type i.e. deposit or transfer
    private final int State_Access_Skewness; // ratio of state access, following zipf distribution
    private final int Transaction_Length; // transaction length, 4 or 8 or longer
    private final int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private final int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    // control the number of txns overlap with each other.
    private final ArrayList<Integer> generatedAcc = new ArrayList<>();
    private final ArrayList<Integer> generatedAst = new ArrayList<>();
    // independent transactions.
    private final boolean isUnique = false;
    private final FastZipfGenerator accountZipf;
    private final FastZipfGenerator assetZipf;
    private final Random random = new Random(0); // the transaction type decider
    private final HashMap<Integer, Integer> nGeneratedAccountIds = new HashMap<>();
    private final HashMap<Integer, Integer> nGeneratedAssetIds = new HashMap<>();
    private final HashMap<Integer, Integer> idToLevel = new HashMap<>();
    public FastZipfGenerator[] partitionedAccountZipf;
    public FastZipfGenerator[] partitionedAssetZipf;
    public transient FastZipfGenerator p_generator; // partition generator
    private int floor_interval;
    private ArrayList<Event> events;
    private int eventID = 0;

    public SLTPGDataGenerator(SLTPGDataGeneratorConfig dataConfig) {
        super(dataConfig);

        // TODO: temporarily hard coded, will update later
        Ratio_Of_Deposit = dataConfig.Ratio_Of_Deposit;//0-100 (%)
        State_Access_Skewness = dataConfig.State_Access_Skewness;
        Transaction_Length = 4;
        Ratio_of_Transaction_Aborts = dataConfig.Ratio_of_Transaction_Aborts;
        Ratio_of_Overlapped_Keys = dataConfig.Ratio_of_Overlapped_Keys;

        int nKeyState = dataConfig.getnKeyStates();

        // allocate levels for each key, to prevent circular.
//        int MAX_LEVEL = (nKeyState / dataConfig.getTotalThreads()) / 2;
        int MAX_LEVEL = 256;
        for (int i = 0; i < nKeyState; i++) {
            idToLevel.put(i, random.nextInt(MAX_LEVEL));
        }

        events = new ArrayList<>(nTuples);
        // zipf state access generator
        accountZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
        assetZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 123456789);
        configure_store(1, (double) State_Access_Skewness / 100, dataConfig.getTotalThreads(), nKeyState);
        p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
    }

    public static void main(String[] args) {
        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(10, 1, 0);
        fastZipfGenerator.show_sample();
    }

    public void configure_store(double scale_factor, double theta, int tthread, int numItems) {
        floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / tthread;
        partitionedAccountZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        partitionedAssetZipf = new FastZipfGenerator[tthread];//overhead_total number of working threads.
        for (int i = 0; i < tthread; i++) {
            partitionedAccountZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 12345678);
            partitionedAssetZipf[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval, 123456789);
        }
    }

    protected void generateTuple() {
        Event event;
        int next = random.nextInt(100);
        if (next < Ratio_Of_Deposit) {
            event = randomDepositEvent();
        } else {
            event = randomTransferEvent();
        }
        events.add(event);
    }

    private Event randomTransferEvent() {
        // make sure source and destination are different
        int srcAcc, dstAcc, srcAst, dstAst;

        if (!isUnique) {
            if (enable_states_partition) {
                int partitionId = key_to_partition(p_generator.next());
                int[] accKeys = getKeys(partitionedAccountZipf[partitionId],
                        partitionedAccountZipf[(partitionId + 2) % dataConfig.getTotalThreads()],
                        partitionId,
                        (partitionId + 2) % dataConfig.getTotalThreads(),
                        generatedAcc);
                srcAcc = accKeys[0];
                dstAcc = accKeys[1];
                int[] astKeys = getKeys(partitionedAssetZipf[(partitionId + 1) % dataConfig.getTotalThreads()],
                        partitionedAssetZipf[(partitionId + 3) % dataConfig.getTotalThreads()],
                        (partitionId + 1) % dataConfig.getTotalThreads(),
                        (partitionId + 3) % dataConfig.getTotalThreads(),
                        generatedAst);
                srcAst = astKeys[0];
                dstAst = astKeys[1];
                assert srcAcc / floor_interval == partitionId;
                assert srcAst / floor_interval == (partitionId + 1) % dataConfig.getTotalThreads();
                assert dstAcc / floor_interval == (partitionId + 2) % dataConfig.getTotalThreads();
                assert dstAst / floor_interval == (partitionId + 3) % dataConfig.getTotalThreads();
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
        Event t;
        if (random.nextInt(10000) < Ratio_of_Transaction_Aborts) {
            t = new SLTransferEvent(eventID, srcAcc, srcAst, dstAcc, dstAst, 100000000, 100000000);
        } else {
            t = new SLTransferEvent(eventID, srcAcc, srcAst, dstAcc, dstAst);
        }

        // increase the timestamp i.e. transaction id
        eventID++;
        return t;
    }

    private Event randomDepositEvent() {
        int acc;
        int ast;
        if (!isUnique) {
            if (enable_states_partition) {
                int partitionId = key_to_partition(p_generator.next());
                acc = getKey(partitionedAccountZipf[partitionId], partitionId, generatedAcc);
                ast = getKey(partitionedAssetZipf[(partitionId + 1) % dataConfig.getTotalThreads()], (partitionId + 1) % dataConfig.getTotalThreads(), generatedAst);
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

        Event t = new SLDepositEvent(eventID, acc, ast);

        // increase the timestamp i.e. transaction id
        eventID++;
        return t;
    }

    public int key_to_partition(int key) {
        return (int) Math.floor((double) key / floor_interval);
    }

    private int[] getKeys(FastZipfGenerator zipfGeneratorSrc, FastZipfGenerator zipfGeneratorDst, int partitionSrc, int partitionDst, ArrayList<Integer> generatedKeys) {
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

    private int[] getKeys(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
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
        if (enable_log) LOG.info("++++++" + nGeneratedAccountIds.size());
        if (enable_log) LOG.info("++++++" + nGeneratedAssetIds.size());

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
