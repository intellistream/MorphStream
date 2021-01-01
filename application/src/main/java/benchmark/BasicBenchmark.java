package benchmark;

import common.collections.OsUtils;
import common.param.sl.TransactionEvent;
import datagenerator.DataGeneratorConfig;
import datagenerator.DataGenerator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import runners.sesameRunner;
import state_engine.storage.SchemaRecord;
import state_engine.storage.TableRecord;
import state_engine.storage.datatype.DataBox;
import state_engine.storage.datatype.LongDataBox;
import state_engine.storage.datatype.StringDataBox;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.util.*;

public class BasicBenchmark implements IBenchmark {

    private String[] args;
    private DataGenerator mDataGenerator;
    private sesameRunner sesameRunner;

    public BasicBenchmark(String[] args) {

        List<String> argslist = new ArrayList<>(Arrays.asList(args)); // Allows to ommit few parameters.
        if(!argslist.contains("--app")) {
            argslist.add("--app");
            argslist.add("StreamLedger");
        }
        if(!argslist.contains("--native")) {
            argslist.add("--native");
            argslist.add("false");
        }
        if(!argslist.contains("--THz")) {
            argslist.add("--THz");
            argslist.add("10000");
        }
        if(!argslist.contains("--machine")) {
            argslist.add("--machine");
            argslist.add("10");
        }
        if(!argslist.contains("--scheduler")) {
            argslist.add("--scheduler");
            argslist.add("BL");
        }
        if(!argslist.contains("--rootFilePath")) { // default path for our current benchmarks
            argslist.add("--rootFilePath");
            argslist.add("/home/hadoop/sesame/data/");
        }
        args = new String[argslist.size()];
        argslist.toArray(args);

        this.args = args;
        createDataGenerator(args);
    }

    protected void createDataGenerator(String[] args) {

        DataGeneratorConfig dataConfig = new DataGeneratorConfig();
        JCommander cmd = new JCommander(dataConfig);
        cmd.setAcceptUnknownOptions(true);
        try {
            cmd.parse(Arrays.copyOf(args, args.length));
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
        }
        dataConfig.updateDependencyLevels();

        dataConfig.idsPath = dataConfig.rootPath;

        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(
                            digest.digest(
                                    String.format("%d_%s", dataConfig.tuplesPerBatch*dataConfig.totalBatches,
                                            Arrays.toString(dataConfig.dependenciesDistributionForLevels))
                                            .getBytes("UTF-8"))));
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataConfig.rootPath += subFolder;

        for(int index=0; index<args.length; index+=2)
            if(args[index].contains("rootFilePath"))
                args[index + 1] = dataConfig.rootPath;

        mDataGenerator = new DataGenerator(dataConfig);
    }

    protected void createSesameRunner(String[] args, int iteration) {

        sesameRunner = new sesameRunner();

        List<String> argslist = new ArrayList<>(Arrays.asList(args)); // Allows to ommit few parameters.
        if(!argslist.contains("--iterationNumber")) {
            argslist.add("--iterationNumber");
            argslist.add(String.format("%d", iteration));
        }

        String[] argsN = new String[argslist.size()];
        argslist.toArray(argsN);
        JCommander cmd = new JCommander(sesameRunner);
        cmd.setAcceptUnknownOptions(true);

        try {
            cmd.parse(Arrays.copyOf(argsN, argsN.length));
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
        }
    }

    public void execute() {

        int totalIterations = 3;

        int tuplesPerBatch = mDataGenerator.getDataConfig().tuplesPerBatch;
        int totalBatches = mDataGenerator.getDataConfig().totalBatches;
        int numberOfLevels = mDataGenerator.getDataConfig().numberOfDLevels;
        int tt = mDataGenerator.getDataConfig().totalThreads;
        boolean shufflingActive = mDataGenerator.getDataConfig().shufflingActive;
        String folder = mDataGenerator.getDataConfig().rootPath;

        String statsFolderPattern = mDataGenerator.getDataConfig().idsPath
                +OsUtils.osWrapperPostFix("stats")
                +OsUtils.osWrapperPostFix("scheduler = %s")
                +OsUtils.osWrapperPostFix("depth = %d")
                +OsUtils.osWrapperPostFix("threads = %d")
                +OsUtils.osWrapperPostFix("total_batches = %d")
                +OsUtils.osWrapperPostFix("events_per_batch = %d");

        String statsFolderPath = String.format(statsFolderPattern, mDataGenerator.getDataConfig().scheduler, numberOfLevels, tt, totalBatches, tuplesPerBatch);
        File file = new File(statsFolderPath+ String.format("iteration_%d.csv", (totalIterations-1)));
        if(!file.exists()) {
//            System.out.println("Stats for following execution already exists at,");
//            System.out.println(statsFolderPath);
//            System.out.println("Please delete them to re-execute the benchmark.");
//            return;

            mDataGenerator.GenerateData();
            mDataGenerator = null;
        }
        loadTransactionEvents(tuplesPerBatch, totalBatches, shufflingActive, folder);

        try {
            for (int lop=0; lop<totalIterations; lop++) {
                createSesameRunner(args, lop);
                sesameRunner.run();
            }
        } catch (InterruptedException ex) {
            System.out.println("Error in running topology locally"+ ex.toString());
        }



    }

    protected void loadTransactionEvents(int tuplesPerBatch, int totalBatches, boolean shufflingActive, String folder) {

        if(DataHolder.events ==null) {

            int numberOfEvents = tuplesPerBatch * totalBatches;
            DataHolder.events = new TransactionEvent[numberOfEvents];
            File file = new File(folder+"transactions.txt");
            if (file.exists()) {
                System.out.println(String.format("Reading transactions..."));
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    String txn = reader.readLine();
                    int count = 0;
                    while (txn!=null) {
                        String[] split = txn.split(",");
                        TransactionEvent event = new TransactionEvent(
                                Integer.parseInt(split[0]), //bid
                                0, //pid
                                String.format("[%s]", split[0]), //bid_array
                                1,//num_of_partition
                                split[1],//getSourceAccountId
                                split[2],//getSourceBookEntryId
                                split[3],//getTargetAccountId
                                split[4],//getTargetBookEntryId
                                100,  //getAccountTransfer
                                100  //getBookEntryTransfer
                        );
                        DataHolder.events[count] = event;
                        count++;
                        if(count%100000==0)
                            System.out.println(String.format("%d transactions read...", count));
                        txn = reader.readLine();
                    }
                    reader.close();
                } catch (Exception e){
                    e.printStackTrace();
                }
                System.out.println(String.format("Done reading transactions..."));

                if(shufflingActive) {
                    Random random = new Random();
                    int index;
                    TransactionEvent temp;
                    for(int lop=0; lop<totalBatches; lop++) {
                        int start   = lop       * tuplesPerBatch;
                        int end     = (lop+1)   * tuplesPerBatch;

                        for (int i = end-1; i > start; i--) {
                            index = start + random.nextInt(i - start + 1);
                            temp = DataHolder.events[index];
                            DataHolder.events[index] = DataHolder.events[i];
                            DataHolder.events[i] = temp;
                        }
                    }
                }
                System.out.println("");
            }
        }
    }

}
