package benchmark.datagenerator;

import benchmark.datagenerator.apps.SL.output.GephiOutputHandler;
import benchmark.datagenerator.apps.SL.output.IOutputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data generator for benchmarks, this class contains all common methods and attributes that can be used in each application
 */
public abstract class DataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    protected final int nTuples;
    protected DataGeneratorConfig dataConfig;
    protected IOutputHandler dataOutputHandler; // dump data to the specified path

    public DataGenerator(DataGeneratorConfig dataConfig) {
        this.dataConfig = dataConfig;
        this.nTuples = dataConfig.getTotalEvents();
        this.dataOutputHandler = new GephiOutputHandler(dataConfig.getRootPath());
    }

    public <T extends DataGeneratorConfig> T getDataConfig() {
        return (T) dataConfig;
    }

    public void generateStream() {
        for (int tupleNumber = 0; tupleNumber < nTuples; tupleNumber++) {
            generateTuple();
        }
    }

    protected abstract void generateTuple();

    public abstract void dumpGeneratedDataToFile();

    public void clearDataStructures() {
        this.dataConfig = null;
    }

}
