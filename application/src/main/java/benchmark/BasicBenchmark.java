package benchmark;

import common.collections.OsUtils;
import datagenerator.DataGeneratorConfig;
import datagenerator.DataGenerator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import runners.sesameRunner;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BasicBenchmark implements IBenchmark {

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
        if(!argslist.contains("--rootFilePath")) { // default path for our current benchmarks
            argslist.add("--rootFilePath");
            argslist.add("/home/hadoop/sesame/data/");
        }
        args = new String[argslist.size()];
        argslist.toArray(args);

        createDataGenerator(args);
        createSesameRunner(args);
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

        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(
                            digest.digest(
                                    String.format("%d_%d_%s", dataConfig.tuplesPerBatch, dataConfig.totalBatches,
                                            Arrays.toString(dataConfig.dependenciesDistributionForLevels))
                                            .getBytes("UTF-8"))));
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataConfig.idsPath= dataConfig.rootPath;
        dataConfig.rootPath+= subFolder;

        System.out.println("Updating root path...");
        for(int index=0; index<args.length; index+=2)
            if(args[index].contains("rootFilePath")) {
                args[index + 1] = dataConfig.rootPath;
                System.out.println(String.format("Updating root path to %s...", args[index+1]));
            }

        System.out.println("Done updating root path...");

        mDataGenerator = new DataGenerator(dataConfig);
    }

    protected void createSesameRunner(String[] args) {
        sesameRunner = new sesameRunner();
        JCommander cmd = new JCommander(sesameRunner);
        cmd.setAcceptUnknownOptions(true);
        try {
            cmd.parse(Arrays.copyOf(args, args.length));
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
        }

    }

    public void execute(){

        mDataGenerator.GenerateData();
        try {
            sesameRunner.run();
        } catch (InterruptedException ex) {
            System.out.println("Error in running topology locally"+ ex.toString());
        }
    }

}
