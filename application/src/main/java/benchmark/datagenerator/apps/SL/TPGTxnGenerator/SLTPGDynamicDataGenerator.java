package benchmark.datagenerator.apps.SL.TPGTxnGenerator;

import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.SL.Transaction.SLDepositEvent;
import benchmark.datagenerator.apps.SL.Transaction.SLTransferEvent;
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

public class SLTPGDynamicDataGenerator extends DynamicWorkloadGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SLTPGDynamicDataGenerator.class);

    private int Ratio_Of_Deposit;  // ratio of state access type i.e. deposit or transfer
    private int State_Access_Skewness; // ratio of state access, following zipf distribution
    private int Transaction_Length; // transaction length, 4 or 8 or longer
    private int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    private int nKeyState;
    // control the number of txns overlap with each other.
    private ArrayList<Integer> generatedAcc = new ArrayList<>();
    private ArrayList<Integer> generatedAst = new ArrayList<>();
    // independent transactions.
    private final boolean isUnique = false;
    private FastZipfGenerator accountZipf;
    private FastZipfGenerator assetZipf;

    private int floor_interval;
    public FastZipfGenerator[] partitionedAccountZipf;
    public FastZipfGenerator[] partitionedAssetZipf;


    private Random random = new Random(0); // the transaction type decider
    public transient FastZipfGenerator p_generator; // partition generator
    private HashMap<Integer, Integer> nGeneratedAccountIds = new HashMap<>();
    private HashMap<Integer, Integer> nGeneratedAssetIds = new HashMap<>();
    private ArrayList<Event> events;
    private int eventID = 0;
    private HashMap<Integer, Integer> idToLevel = new HashMap<>();
    public SLTPGDynamicDataGenerator(DynamicDataGeneratorConfig dynamicDataConfig){
        super(dynamicDataConfig);
        events = new ArrayList<>(nTuples);
    }

    @Override
    public void mapToTPGProperties() {
        //TD,LD,PD,VDD,Skew,R_of_A,isCD,isCC,
        StringBuilder stringBuilder = new StringBuilder();
        //TODO:hard code, function not sure
        double td = Transaction_Length * dynamicDataConfig.getCheckpoint_interval() * (1 - (double)Ratio_Of_Deposit/100) +
                ((double)Ratio_Of_Deposit/100) * 2 * dynamicDataConfig.getCheckpoint_interval();
        td = td *((double) Ratio_of_Overlapped_Keys/100);
        stringBuilder.append(td);
        stringBuilder.append(",");
        double ld = Transaction_Length * dynamicDataConfig.getCheckpoint_interval() * (1 - (double)Ratio_Of_Deposit/100) +
                ((double)Ratio_Of_Deposit/100) * 2 * dynamicDataConfig.getCheckpoint_interval();
        stringBuilder.append(ld);
        stringBuilder.append(",");
        double pd = (1 - (double)Ratio_Of_Deposit/100) * Transaction_Length * dynamicDataConfig.getCheckpoint_interval() * 2 * ((double) Ratio_of_Overlapped_Keys/100);
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
        switch (type) {
            case "default" :
                Ratio_Of_Deposit = dynamicDataConfig.Ratio_Of_Deposit;//0-100 (%)
                State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
                Transaction_Length = 4;
                Ratio_of_Transaction_Aborts = dynamicDataConfig.Ratio_of_Transaction_Aborts;
                Ratio_of_Overlapped_Keys = dynamicDataConfig.Ratio_of_Overlapped_Keys;

                nKeyState = dynamicDataConfig.getnKeyStates();
                // allocate levels for each key, to prevent circular.
//        int MAX_LEVEL = (nKeyState / dataConfig.getTotalThreads()) / 2;
                int MAX_LEVEL = 256;
                for (int i = 0; i < nKeyState; i++) {
                    idToLevel.put(i, random.nextInt(MAX_LEVEL));
                }
                // zipf state access generator
                accountZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                assetZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 123456789);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
            break;
            case "skew" :
                State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
                accountZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                assetZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 123456789);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
            break;
            case "PD" :
                Ratio_Of_Deposit = dynamicDataConfig.Ratio_Of_Deposit;//0-100 (%)
                if (Ratio_Of_Deposit < 45) {
                    AppConfig.isCyclic = true;
                }
            break;
            case "abort":
                Ratio_of_Transaction_Aborts = dynamicDataConfig.Ratio_of_Transaction_Aborts;
            break;
            case "unchanging" :
            break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        mapToTPGProperties();
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
    @Override
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
    @Override
    public void dumpGeneratedDataToFile() {
        if (enable_log) LOG.info("++++++" + nGeneratedAccountIds.size());
        if (enable_log) LOG.info("++++++" + nGeneratedAssetIds.size());

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
    private Event randomTransferEvent() {
        // make sure source and destination are different
        int srcAcc, dstAcc, srcAst, dstAst;

        if (!isUnique) {
            if (enable_states_partition) {
                int partitionId = key_to_partition(p_generator.next());
                int[] accKeys = getKeys(partitionedAccountZipf[partitionId],
                        partitionedAccountZipf[(partitionId+2) % dynamicDataConfig.getTotalThreads()],
                        partitionId,
                        (partitionId+2) % dynamicDataConfig.getTotalThreads(),
                        generatedAcc);
                srcAcc = accKeys[0];
                dstAcc = accKeys[1];
                int[] astKeys = getKeys(partitionedAssetZipf[(partitionId+1) % dynamicDataConfig.getTotalThreads()],
                        partitionedAssetZipf[(partitionId+3) % dynamicDataConfig.getTotalThreads()],
                        (partitionId+1) % dynamicDataConfig.getTotalThreads(),
                        (partitionId+3) % dynamicDataConfig.getTotalThreads(),
                        generatedAst);
                srcAst = astKeys[0];
                dstAst = astKeys[1];
                assert srcAcc / floor_interval == partitionId;
                assert srcAst / floor_interval == (partitionId+1) % dynamicDataConfig.getTotalThreads();
                assert dstAcc / floor_interval == (partitionId+2) % dynamicDataConfig.getTotalThreads();
                assert dstAst / floor_interval == (partitionId+3) % dynamicDataConfig.getTotalThreads();
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
        if(!isUnique) {
            if (enable_states_partition) {
                int partitionId = key_to_partition(p_generator.next());
                acc = getKey(partitionedAccountZipf[partitionId], partitionId, generatedAcc);
                ast = getKey(partitionedAssetZipf[(partitionId+1) % dynamicDataConfig.getTotalThreads()], (partitionId+1) % dynamicDataConfig.getTotalThreads(), generatedAst);
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
}
