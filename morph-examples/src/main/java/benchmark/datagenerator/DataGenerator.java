package benchmark.datagenerator;

import benchmark.datagenerator.apps.SL.output.GephiOutputHandler;
import benchmark.datagenerator.apps.SL.output.IOutputHandler;
import benchmark.dynamicWorkloadGenerator.DynamicDataGeneratorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Data generator for benchmarks, this class contains all common methods and attributes that can be used in each application
 */
public abstract class DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    protected final int nTuples;
    protected DataGeneratorConfig dataConfig;
    protected IOutputHandler dataOutputHandler; // dump data to the specified path
    protected int windowCount = 0;


    public DataGenerator(DataGeneratorConfig dataConfig) {
        this.dataConfig = dataConfig;
        this.nTuples = dataConfig.getTotalEvents();
        this.dataOutputHandler = new GephiOutputHandler(dataConfig.getRootPath());
    }

    public DataGenerator(DynamicDataGeneratorConfig dynamicDataConfig) {
        this.nTuples = dynamicDataConfig.getTotalEvents();
        this.dataOutputHandler = new GephiOutputHandler(dynamicDataConfig.getRootPath());
    }

    public <T extends DataGeneratorConfig> T getDataConfig() {
        return (T) dataConfig;
    }

    public void generateStream() {
        for (int tupleNumber = 0; tupleNumber < nTuples + dataConfig.getTotalThreads(); tupleNumber++) {//add a padding to avoid non-integral-divided problem.
            generateTuple();
        }
        System.out.println(windowCount);
    }

    protected abstract void generateTuple();

    public abstract void dumpGeneratedDataToFile();

    public void clearDataStructures() {
        this.dataConfig = null;
    }
    /* Switch configuration, used in the dynamic data generator*/

    public List<String> getTranToDecisionConf() {
        return null;
    }

    public void generateTPGProperties() {
    }

}
