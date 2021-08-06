package benchmark.datagenerator;

import benchmark.datagenerator.apps.SL.output.GephiOutputHandler;
import benchmark.datagenerator.apps.SL.output.IOutputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Data generator for benchmarks, this class contains all common methods and attributes that can be used in each application
 */
public abstract class SpecialDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(SpecialDataGenerator.class);

    protected final int nTuples;
    protected DataGeneratorConfig dataConfig;

    protected IOutputHandler dataOutputHandler; // dump data to the specified path

    public SpecialDataGenerator(DataGeneratorConfig dataConfig) {
        this.dataConfig = dataConfig;
        this.nTuples = dataConfig.getTuplesPerBatch() * dataConfig.getTotalBatches();
        this.dataOutputHandler = new GephiOutputHandler(dataConfig.getRootPath());
    }

    public <T extends DataGeneratorConfig> T getDataConfig() {
        return (T) dataConfig;
    }

    public void generateStream() {
        // if file is already exist, skip generation
        if (isFileExist()) return;

        for (int tupleNumber = 0; tupleNumber < nTuples; tupleNumber++) {
            generateTuple();
        }

        LOG.info(String.format("Data Generator will dump data at %s.", dataConfig.getRootPath()));
        dumpGeneratedDataToFile();
        LOG.info("Data Generation is done...");
        clearDataStructures();
        this.dataConfig = null;
    }

    protected boolean isFileExist() {
        File file = new File(dataConfig.getRootPath());
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
            LOG.info(dataConfig.getRootPath());
            return true;
        }
        return false;
    }

    /**
     * generate a set of operations, group them as OC and construct them as OC graph, then create txn from the created OCs.
     */
    protected void generateTuple() {
        // Step 1: select OCs for txn according to the required OCs dependency distribution
        // Step 2: update OCs dependencies graph for future data generation
        // Step 3: create txn with the selected OCs, the specific operations are generated inside.
        // Step 4: update the statistics such as dependency distribution to guide future data generation
    };

    protected abstract void dumpGeneratedDataToFile();

    protected void clearDataStructures() {
        this.dataConfig = null;
    };
}
