package benchmark;

import benchmark.datagenerator.DatGeneratorConfig;
import benchmark.datagenerator.DataGenerator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import common.collections.OsUtils;

import runners.sesameRunner;

public class BasicBenchmark implements IBenchmark {

    private DataGenerator mDataGenerator;
    private sesameRunner sesameRunner;

    public BasicBenchmark(String[] args) {

        DatGeneratorConfig dataConfig = new DatGeneratorConfig();
        JCommander cmd = new JCommander(dataConfig);
        try {
//            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
        }

        if(dataConfig.rootPath==null)
            dataConfig.rootPath = System.getProperty("user.home") + OsUtils.OS_wrapper("sesame") + OsUtils.OS_wrapper("SYNTH_DATA/");
        dataConfig.updateDependencyLevels();
        mDataGenerator = new DataGenerator(dataConfig);

        sesameRunner = new sesameRunner();
        cmd = new JCommander(sesameRunner);
        try {
            cmd.parse(args);
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
