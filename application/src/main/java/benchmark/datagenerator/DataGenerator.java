package benchmark.datagenerator;

import benchmark.datagenerator.old.DataGeneratorConfig;
import benchmark.datagenerator.old.DataOperationChain;
import benchmark.datagenerator.old.DataTransaction;
import benchmark.datagenerator.output.GephiOutputHandler;
import benchmark.datagenerator.output.IOutputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

/**
 * Data generator for benchmarks, this class contains all common methods and attributes that can be used in each application
 */
public abstract class DataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    protected final int mTotalTuplesToGenerate;
    protected DataGeneratorConfig dataConfig;
    protected ArrayList<DataTransaction> mDataTransactions;

    protected long mPartitionOffset = 0; // TODO: not sure what is this used for, a confusing config...

    protected IOutputHandler mDataOutputHandler; // dump data to the specified path

    protected int mTransactionId = 0;

    protected long selectTuples = 0;
    protected long updateDependency = 0;

    // metrics related attributes
    protected long totalTimeStart = 0;
    protected long totalTime = 0;
    protected long selectTuplesStart = 0;
    protected long updateDependencyStart = 0;

    public DataGenerator(DataGeneratorConfig dataConfig) {
        this.dataConfig = dataConfig;
        this.mTotalTuplesToGenerate = dataConfig.tuplesPerBatch * dataConfig.totalBatches;
        this.mDataTransactions = new ArrayList<>(mTotalTuplesToGenerate);
        this.mPartitionOffset = (mTotalTuplesToGenerate * 5) / dataConfig.totalThreads; // TODO: why * 5 ?

        this.mDataOutputHandler = new GephiOutputHandler(dataConfig.rootPath);
    }

    public DataGeneratorConfig getDataConfig() {
        return dataConfig;
    }

    public void GenerateStream() {

        File file = new File(dataConfig.rootPath);
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
            LOG.info(dataConfig.rootPath);
            return;
        }

        LOG.info(String.format("Data Generator will dump data at %s.", dataConfig.rootPath));

        for (int tupleNumber = 0; tupleNumber < mTotalTuplesToGenerate / 10; tupleNumber++) { // TODO: why / 10?
            generateTuple();
        }
        dumpGeneratedDataToFile();
        LOG.info("Data Generation is done...");
        clearDataStructures();
        this.dataConfig = null;
    }

    /**
     * generate a set of operations, group them as OC and construct them as OC graph, then create txn from the created OCs.
     */
    protected void generateTuple() {
        // 1. create a set of OC progressively, each one contain a key state of the application (this step is not pre-defined)
        // 2. try to construct a layered OC dependency graph by generating Operations in each OC.
        // 3. use the selected operations to construct Transactions

    };

    protected abstract void dumpGeneratedDataToFile();

    protected abstract void clearDataStructures();
}
