package benchmark.datagenerator.apps.GS;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.apps.SL.SLDataOperationChain;
import benchmark.datagenerator.apps.SL.SLDataTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class GSDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    HashMap<Long, Integer> mGeneratedAccountIds = new HashMap<>();
    HashMap<Long, Integer> mGeneratedAssetIds = new HashMap<>();
    private Random mRandomGenerator = new Random();
    private Random mRandomGeneratorForAccIds = new Random(12345678);
    private Random mRandomGeneratorForAstIds = new Random(123456789);
    private int totalAccountRecords = 0;
    private int totalAssetRecords = 0;
    private ArrayList<SLDataTransaction> mDataTransactions;
    private HashMap<Integer, ArrayList<SLDataOperationChain>> mAccountOperationChainsByLevel;
    private HashMap<Integer, ArrayList<SLDataOperationChain>> mAssetsOperationChainsByLevel;
    private float[] mAccountLevelsDistribution;
    private float[] mAssetLevelsDistribution;
    private float[] mOcLevelsDistribution;
    private boolean[] mPickAccount;
    private int mTransactionId = 0;
    private long totalTimeStart = 0;
    private long totalTime = 0;
    private long selectTuplesStart = 0;
    private long selectTuples = 0;
    private long updateDependencyStart = 0;
    private long updateDependency = 0;

    private long mPartitionOffset = 0;
    private int mPId = 0;

    SLDataOperationChain srcAccOC = null;
    SLDataOperationChain srcAstOC = null;
    SLDataOperationChain dstAccOC = null;
    SLDataOperationChain dstAstOC = null;

    public GSDataGenerator(DataGeneratorConfig dataConfig) {
        super(dataConfig);
        this.mDataTransactions = new ArrayList<>(mTotalTuplesToGenerate);
        this.mAccountOperationChainsByLevel = new HashMap<>();
        this.mAssetsOperationChainsByLevel = new HashMap<>();
        this.mAccountLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mAssetLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mOcLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mPickAccount = new boolean[dataConfig.dependenciesDistributionForLevels.length];
        this.mPartitionOffset = (mTotalTuplesToGenerate * 5) / dataConfig.totalThreads;
    }

    @Override
    protected void generateTuple() {
        // Step 1: select OCs for txn according to the required OCs dependency distribution

        // Step 2: update OCs dependencies graph for future data generation

        // Step 3: create txn with the selected OCs, the specific operations are generated inside.

        // Step 4: update the statistics such as dependency distribution to guide future data generation
    }

    @Override
    protected void dumpGeneratedDataToFile() {

        File file = new File(dataConfig.rootPath);
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
            return;
        }
        file.mkdirs();

        File versionFile = new File(dataConfig.rootPath.substring(0, dataConfig.rootPath.length() - 1)
                + String.format("_%d_%d_%d.txt", dataConfig.tuplesPerBatch, dataConfig.totalBatches, dataConfig.numberOfDLevels));
        try {
            versionFile.createNewFile();
            FileWriter fileWriter = new FileWriter(versionFile);
            fileWriter.write(String.format("Tuples per batch      : %d\n", dataConfig.tuplesPerBatch));
            fileWriter.write(String.format("Total batches         : %d\n", dataConfig.totalBatches));
            fileWriter.write(String.format("Dependency depth      : %d\n", dataConfig.numberOfDLevels));
            fileWriter.write(String.format("%s\n", Arrays.toString(mOcLevelsDistribution)));
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOG.info(String.format("Dumping transactions..."));
        mDataOutputHandler.sinkTransactions(mDataTransactions);
//         LOG.info(String.format("Dumping Dependency Edges..."));
//        mDataOutputHandler.sinkDependenciesEdges(mAccountOperationChainsByLevel, mAssetsOperationChainsByLevel);
        LOG.info(String.format("Dumping Dependency Vertices..."));
        mDataOutputHandler.sinkDependenciesVertices(mAccountOperationChainsByLevel, mAssetsOperationChainsByLevel);
        LOG.info(String.format("Dumping Dependency Vertices ids range..."));
        mDataOutputHandler.sinkDependenciesVerticesIdsRange(totalAccountRecords, totalAssetRecords);
    }

    @Override
    protected void clearDataStructures() {

        if (mDataTransactions != null) {
            mDataTransactions.clear();
        }
        mDataTransactions = new ArrayList<>();

        if (mAccountOperationChainsByLevel != null) {
            mAccountOperationChainsByLevel.clear();
        }
        mAccountOperationChainsByLevel = new HashMap<>();

        if (mAssetsOperationChainsByLevel != null) {
            mAssetsOperationChainsByLevel.clear();
        }
        mAssetsOperationChainsByLevel = new HashMap<>();

        this.mAccountLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mAssetLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mOcLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mPickAccount = new boolean[dataConfig.numberOfDLevels];
    }
}
