package benchmark.datagenerator.apps.LB.TPGTxnGenerator;

import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.LB.TPGTxnGenerator.Transaction.LBEvent;
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
import java.util.concurrent.ThreadLocalRandom;

import static common.CONTROL.enable_log;

public class LBTPGDynamicDataGenerator extends DynamicWorkloadGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(LBTPGDynamicDataGenerator.class);
    private int NUM_ACCESS; // transaction length, 4 or 8 or longer
    private int State_Access_Skewness; // ratio of state access, following zipf distribution
    private int Ratio_of_Windowed_Reads; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    private int Transaction_Length;
    private int Ratio_of_Multiple_State_Access;//ratio of multiple state access per transaction
    private int tthread;
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

    public LBTPGDynamicDataGenerator(DynamicDataGeneratorConfig dynamicDataConfig) {
        super(dynamicDataConfig);
        events = new ArrayList<>(nTuples);
    }

    @Override
    public void mapToTPGProperties() {
        //TD,LD,PD,VDD,R_of_A,isCD,isCC,
        StringBuilder stringBuilder = new StringBuilder();
        //TODO:hard code, function not sure
        double td = Transaction_Length * dynamicDataConfig.getCheckpoint_interval();
        td = td *((double) Ratio_of_Overlapped_Keys/100);
        stringBuilder.append(td);
        stringBuilder.append(",");
        double ld = Transaction_Length * dynamicDataConfig.getCheckpoint_interval();
        stringBuilder.append(ld);
        stringBuilder.append(",");
        double pd = Transaction_Length * dynamicDataConfig.getCheckpoint_interval() * ((double) Ratio_of_Overlapped_Keys/100) * NUM_ACCESS;
        stringBuilder.append(pd);
        stringBuilder.append(",");
        stringBuilder.append((double) State_Access_Skewness/100);
        stringBuilder.append(",");
        stringBuilder.append((double) Ratio_of_Windowed_Reads /10000);
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
                State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
                NUM_ACCESS = dynamicDataConfig.NUM_ACCESS;
                Ratio_of_Windowed_Reads = dynamicDataConfig.Ratio_of_Transaction_Aborts;
                Ratio_of_Overlapped_Keys = dynamicDataConfig.Ratio_of_Overlapped_Keys;
                Transaction_Length = dynamicDataConfig.Transaction_Length;
                Ratio_of_Multiple_State_Access = dynamicDataConfig.Ratio_of_Multiple_State_Access;

                nKeyState = dynamicDataConfig.getnKeyStates();
                int MAX_LEVEL = 256;
                for (int i = 0; i < nKeyState; i++) {
                    idToLevel.put(i, i% MAX_LEVEL);
                }
                keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
                break;
            case "LD" :
                Transaction_Length = dynamicDataConfig.Transaction_Length;
                break;
            case "isCyclic" :
                Ratio_of_Windowed_Reads = dynamicDataConfig.Ratio_of_Transaction_Aborts;
                State_Access_Skewness = dynamicDataConfig.State_Access_Skewness;
                keyZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
                configure_store(1, (double) State_Access_Skewness / 100, dynamicDataConfig.getTotalThreads(), nKeyState);
                p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
                AppConfig.isCyclic = true;
                break;
            case "complexity" :
                AppConfig.complexity = 80000;
                break;
            case "windowSize" :
                AppConfig.windowSize = 1024;
            case "unchanging" :
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        mapToTPGProperties();
    }

    @Override
    protected void generateTuple() {
        LBEvent event = randomEvent();
        events.add(event);
    }


    private LBEvent randomEvent() {
        int randomKey = ThreadLocalRandom.current().nextInt(0, 100); //TODO: Change this
        String defaultStr = "default";

        LBEvent t = new LBEvent(eventID, randomKey, defaultStr, defaultStr, defaultStr, defaultStr);

        eventID++;
        return t;
    }


    //Copied from GSW, Method used during randomEvent generation
    public int key_to_partition(int key) {
        return (int) Math.floor((double) key / floor_interval);
    }

    //Copied from GSW, Method used during randomEvent generation
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

    //Copied from GSW, Method used during randomEvent generation
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

    //Copied from GSW, Method used during randomEvent generation
    private int getUniqueKey(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
        int key;
        key = zipfGenerator.next();
        while (generatedKeys.contains(key)) {
            key = zipfGenerator.next();
        }
        generatedKeys.add(key);
        return key;
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


}
