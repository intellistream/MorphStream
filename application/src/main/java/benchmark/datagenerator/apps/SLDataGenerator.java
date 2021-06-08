package benchmark.datagenerator.apps;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.old.DataGeneratorConfig;
import benchmark.datagenerator.old.DataOperationChain;
import benchmark.datagenerator.old.DataTransaction;
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

public class SLDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    HashMap<Long, Integer> mGeneratedAccountIds = new HashMap<>();
    HashMap<Long, Integer> mGeneratedAssetIds = new HashMap<>();
    private Random mRandomGenerator = new Random();
    private Random mRandomGeneratorForAccIds = new Random(12345678);
    private Random mRandomGeneratorForAstIds = new Random(123456789);
    private int totalAccountRecords = 0;
    private int totalAssetRecords = 0;
    private int mTotalTuplesToGenerate;
    private DataGeneratorConfig dataConfig;
    private ArrayList<DataTransaction> mDataTransactions;
    private HashMap<Integer, ArrayList<DataOperationChain>> mAccountOperationChainsByLevel;
    private HashMap<Integer, ArrayList<DataOperationChain>> mAssetsOperationChainsByLevel;
    private IOutputHandler mDataOutputHandler;
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

    public SLDataGenerator(DataGeneratorConfig dataConfig) {
        super(dataConfig);
    }

    @Override
    protected void generateTuple() {

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

    private DataOperationChain getExistingIndependentDestOC(HashMap<Integer, ArrayList<DataOperationChain>> allOcs, DataOperationChain srcAcc, DataOperationChain srcAst) {

        ArrayList<DataOperationChain> independentOcs = allOcs.get(0);
        if (independentOcs == null || independentOcs.size() == 0)
            return null;

        DataOperationChain oc = null;
        for (int lop = independentOcs.size() - 1; lop >= 0; lop--) {
            oc = independentOcs.get(lop);
            // check the OC with 4 conditions:
            // 1. whether has children depend on it
            // 2. OC is not equal to srcOC
            // 3. OC is not equal to dstOC
            // 4. OC is in the same partition with mPId?
            // The main goal of this check is to make sure we have picked a independent OC
            // The selected OC is totally "independent" without parent and children.
            if (!oc.hasChildren() &&
                    oc != srcAcc &&
                    oc != srcAst &&
                    !srcAcc.isDependUpon(oc) &&
                    !srcAst.isDependUpon(oc)
                    && (oc.getId() % mPartitionOffset) == mPId)
                break;
            if (independentOcs.size() - lop > 100) {
                oc = null;
                break;
            }
            oc = null;
        }
        return oc;

    }

    private DataOperationChain getRandomExistingOC(int selectionLevel, HashMap<Integer, ArrayList<DataOperationChain>> ocs) {

        ArrayList<DataOperationChain> selectedLevelFilteredOCs = null;
        if (ocs.containsKey(selectionLevel))
            selectedLevelFilteredOCs = ocs.get(selectionLevel);

        DataOperationChain oc = null;
        if (selectedLevelFilteredOCs != null && selectedLevelFilteredOCs.size() > 0) {
            oc = selectedLevelFilteredOCs.get(mRandomGenerator.nextInt(selectedLevelFilteredOCs.size()));
        } else {
            oc = null;
        }
        if (oc != null && (oc.getId() % mPartitionOffset) == mPId)
            oc = null;
        return oc;
    }

    private DataOperationChain getNewAccountOC() {

        long id = 0;
//        int range = 10 * mTotalTuplesToGenerate * 5;
        int range = (int) mPartitionOffset;
        if (dataConfig.idGenType.equals("uniform")) {
            id = mRandomGeneratorForAccIds.nextInt(range);
            id += mPartitionOffset * mPId;
            id *= 10;
            totalAccountRecords++;
            while (mGeneratedAccountIds.containsKey(id)) {
                id = mRandomGeneratorForAccIds.nextInt(range);
                id += mPartitionOffset * mPId;
                id *= 10;
                totalAccountRecords++;
            }
        } else if (dataConfig.idGenType.equals("normal")) {
            id = (int) Math.floor(Math.abs(mRandomGeneratorForAccIds.nextGaussian() / 3.5) * range) % range;
            id += mPartitionOffset * mPId;
            id *= 10;
            totalAccountRecords++;
            while (mGeneratedAccountIds.containsKey(id)) {
                id = (int) Math.floor(Math.abs(mRandomGeneratorForAccIds.nextGaussian() / 3.5) * range) % range;
                id += mPartitionOffset * mPId;
                id *= 10;
                totalAccountRecords++;
            }
        }

        mGeneratedAccountIds.put(id, null);
        DataOperationChain oc = new DataOperationChain("act_" + id, (10 * mTotalTuplesToGenerate * 5) / dataConfig.numberOfDLevels, mAccountOperationChainsByLevel);

        return oc;
    }

    private DataOperationChain getNewAssetOC() {

        long id = 0;
//        int range = 10 * mTotalTuplesToGenerate * 5;
        int range = (int) mPartitionOffset;
        if (dataConfig.idGenType.equals("uniform")) {
            id = mRandomGeneratorForAstIds.nextInt(range);
            id += mPartitionOffset * mPId;
            id *= 10;
            totalAssetRecords++;
            while (mGeneratedAssetIds.containsKey(id)) {
                id = mRandomGeneratorForAstIds.nextInt(range);
                id += mPartitionOffset * mPId;
                id *= 10;
                totalAssetRecords++;
            }
        } else if (dataConfig.idGenType.equals("normal")) {
            id = (int) Math.floor(Math.abs(mRandomGeneratorForAstIds.nextGaussian() / 3.5) * range) % range;
            id += mPartitionOffset * mPId;
            id *= 10;
            totalAssetRecords++;
            while (mGeneratedAssetIds.containsKey(id)) {
                id = (int) Math.floor(Math.abs(mRandomGeneratorForAstIds.nextGaussian() / 3.5) * range) % range;
                id += mPartitionOffset * mPId;
                id *= 10;
                totalAssetRecords++;
            }
        }
        mGeneratedAssetIds.put(id, null);
        DataOperationChain oc = new DataOperationChain("ast_" + id, (10 * mTotalTuplesToGenerate * 5) / dataConfig.numberOfDLevels, mAssetsOperationChainsByLevel);
        return oc;
    }
}
