package benchmark.datagenerator;

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
 * Data generator for benchmarks
 * TODO: refactor with an abstract DataGenerator, design different data generator for all apps.
 */
public class DataGenerator {

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


    public DataGenerator(DataGeneratorConfig dataConfig) {
        this.dataConfig = dataConfig;
        this.mTotalTuplesToGenerate = dataConfig.tuplesPerBatch * dataConfig.totalBatches;
        this.mDataTransactions = new ArrayList<>(mTotalTuplesToGenerate);
        this.mAccountOperationChainsByLevel = new HashMap<>();
        this.mAssetsOperationChainsByLevel = new HashMap<>();
        this.mAccountLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mAssetLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mOcLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mPickAccount = new boolean[dataConfig.dependenciesDistributionForLevels.length];
        this.mPartitionOffset = (mTotalTuplesToGenerate * 5) / dataConfig.totalThreads;
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

        mDataOutputHandler = new GephiOutputHandler(dataConfig.rootPath);

        LOG.info(String.format("Data Generator will dump data at %s.", dataConfig.rootPath));
        for (int tupleNumber = 0; tupleNumber < mTotalTuplesToGenerate / 10; tupleNumber++) {
            totalTimeStart = System.nanoTime();
            GenerateTuple();
            totalTime += System.nanoTime() - totalTimeStart;
            UpdateStats();

            if (mTransactionId % 100000 == 0) {
                float selectTuplesPer = (selectTuples * 1.0f) / (totalTime * 1.0f) * 100.0f;
                float updateDependencyPer = (updateDependency * 1.0f) / (totalTime * 1.0f) * 100.0f;
                LOG.info(String.format("Dependency Distribution...select tuple time: %.3f%%, update dependency time: %.3f%%", selectTuplesPer, updateDependencyPer));

                for (int lop = 0; lop < mOcLevelsDistribution.length; lop++) {
                    System.out.print(lop + ": " + mOcLevelsDistribution[lop] + "; ");
                }
                LOG.info(" ");
            }
        }
        dumpGeneratedDataToFile();
        LOG.info("Date Generation is done...");
        clearDataStructures();
        this.dataConfig = null;
    }

    private void GenerateTuple() {

        DataOperationChain srcAccOC = null;
        DataOperationChain srcAstOC = null;
        DataOperationChain dstAccOC = null;
        DataOperationChain dstAstOC = null;

        // 8 possible types of data dependencies in SL
        boolean srcAcc_dependsUpon_srcAst = false;
        boolean srcAst_dependsUpon_srcAcc = false;

        boolean dstAst_dependsUpon_dstAcc = false;
        boolean dstAcc_dependsUpon_dstAst = false;

        boolean dstAst_dependsUpon_srcAst = false;
        boolean dstAst_dependsUpon_srcAcc = false;

        boolean dstAcc_dependsUpon_srcAst = false;
        boolean dstAcc_dependsUpon_srcAcc = false;


        selectTuplesStart = System.nanoTime();
        // step 1: try to check whether the level 0 is generated properly i.e. has similar data distribution as expected
        // if smaller, generate new tuple for level 0, which only need to create new ast and acc
        // if bigger, generate higher level tuples.
        if (mOcLevelsDistribution[0] >= dataConfig.dependenciesDistributionForLevels[0]) {

            // try to select a dependency level for dependency construction
            // simply select the last level of distribution that is bigger as expected
            // e.g. cur distribution: [0.28,0.25,0.25,0.22], expected: [0.25, 0.25, 0.25, 0.25]
            // the selected dependency level is 2.
            int selectedLevel = 0;
            for (int lop = 1; lop < dataConfig.numberOfDLevels; lop++) {
                if (mOcLevelsDistribution[lop] < dataConfig.dependenciesDistributionForLevels[lop]) {
                    selectedLevel = lop - 1;
                    break;
                }
            }

            System.out.println("++++++");
//            System.out.println(mOcLevelsDistribution[0]
//                    + "," + mOcLevelsDistribution[1]
//                    + "," + mOcLevelsDistribution[2]
//                    + "," + mOcLevelsDistribution[3]);
//            System.out.println(dataConfig.dependenciesDistributionForLevels[0]
//                    + "," + dataConfig.dependenciesDistributionForLevels[1]
//                    + "," + dataConfig.dependenciesDistributionForLevels[2]
//                    + "," + dataConfig.dependenciesDistributionForLevels[3]);
//            System.out.println(dataConfig.dependenciesDistributionForLevels[0]
//                    + "," + dataConfig.dependenciesDistributionForLevels[1]
//                    + "," + dataConfig.dependenciesDistributionForLevels[2]
//                    + "," + dataConfig.dependenciesDistributionForLevels[3]);
//            System.out.println(mPickAccount[0]
//                    + "," + mPickAccount[1]
//                    + "," + mPickAccount[2]
//                    + "," + mPickAccount[3]);
//            System.out.println(selectedLevel);


            mPId = mRandomGenerator.nextInt(dataConfig.totalThreads);

            if (mPickAccount[selectedLevel]) {

                System.out.println("++++++ select from existing account");

                // pick a existing OC of a typical level from account oc chains map
                srcAccOC = getRandomExistingOC(selectedLevel, mAccountOperationChainsByLevel);
                if (srcAccOC == null)
                    srcAccOC = getNewAccountOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                // generate a new asset OC for processing
                srcAstOC = getNewAssetOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                // TODO: dst acc oc is always null...
                dstAccOC = getRandomExistingDestOC(mAccountOperationChainsByLevel, srcAccOC, srcAstOC);
                if (dstAccOC == null)
                    dstAccOC = getNewAccountOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                // TODO: ast ast oc is always null...
                dstAstOC = getRandomExistingDestOC(mAssetsOperationChainsByLevel, srcAccOC, srcAstOC);
                if (dstAstOC == null)
                    dstAstOC = getNewAssetOC();

            } else {

                System.out.println("++++++ create a new account at that level");

                srcAccOC = getNewAccountOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                srcAstOC = getRandomExistingOC(selectedLevel, mAssetsOperationChainsByLevel);
                if (srcAstOC == null)
                    srcAstOC = getNewAssetOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                // TODO: dst acc oc is always null...
                dstAccOC = getRandomExistingDestOC(mAccountOperationChainsByLevel, srcAccOC, srcAstOC);
                if (dstAccOC == null)
                    System.out.println("++++++ create new dst acc oc");
                    dstAccOC = getNewAccountOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                // TODO: dst ast oc is always null...
                dstAstOC = getRandomExistingDestOC(mAssetsOperationChainsByLevel, srcAccOC, srcAstOC);
                if (dstAstOC == null)
                    System.out.println("++++++ create new dst ast oc");
                    dstAstOC = getNewAssetOC();
            }

        } else {
            srcAccOC = getNewAccountOC();
            srcAstOC = getNewAssetOC();
            dstAccOC = getNewAccountOC();
            dstAstOC = getNewAssetOC();
        }
        selectTuples += System.nanoTime() - selectTuplesStart;

        dstAcc_dependsUpon_dstAst = dstAccOC.isDependUpon(dstAstOC);
        dstAst_dependsUpon_dstAcc = dstAstOC.isDependUpon(dstAccOC);

        // register dependencies for srcAssets
        if (srcAccOC.getOperationsCount() > 0) {
            srcAst_dependsUpon_srcAcc = srcAstOC.isDependUpon(srcAccOC);
            dstAcc_dependsUpon_srcAcc = dstAccOC.isDependUpon(srcAccOC);
            dstAst_dependsUpon_srcAcc = dstAstOC.isDependUpon(srcAccOC);

            if (!srcAst_dependsUpon_srcAcc) {
                srcAccOC.addChildren(srcAstOC);
                srcAstOC.addParent(srcAccOC);
                updateDependencyStart = System.nanoTime();
                srcAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if (!dstAcc_dependsUpon_srcAcc && !dstAcc_dependsUpon_dstAst) {
                srcAccOC.addChildren(dstAccOC);
                dstAccOC.addParent(srcAccOC);
                updateDependencyStart = System.nanoTime();
                dstAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if (!dstAst_dependsUpon_srcAcc && !dstAst_dependsUpon_dstAcc) {
                srcAccOC.addChildren(dstAstOC);
                dstAstOC.addParent(srcAccOC);
                updateDependencyStart = System.nanoTime();
                dstAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

        } else if (srcAstOC.getOperationsCount() > 0) {
            srcAcc_dependsUpon_srcAst = srcAstOC.isDependUpon(srcAstOC);
            dstAcc_dependsUpon_srcAst = dstAccOC.isDependUpon(srcAstOC);
            dstAst_dependsUpon_srcAst = dstAstOC.isDependUpon(srcAstOC);

            if (!srcAcc_dependsUpon_srcAst) {
                srcAstOC.addChildren(srcAccOC);
                srcAccOC.addParent(srcAstOC);
                updateDependencyStart = System.nanoTime();
                srcAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if (!dstAcc_dependsUpon_srcAst && !dstAcc_dependsUpon_dstAst) {
                srcAstOC.addChildren(dstAccOC);
                dstAccOC.addParent(srcAstOC);
                updateDependencyStart = System.nanoTime();
                dstAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if (!dstAst_dependsUpon_srcAst && !dstAst_dependsUpon_dstAcc) {
                srcAstOC.addChildren(dstAstOC);
                dstAstOC.addParent(srcAstOC);
                updateDependencyStart = System.nanoTime();
                dstAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }
        }

        srcAccOC.addAnOperation();
        srcAstOC.addAnOperation();
        dstAccOC.addAnOperation();
        dstAstOC.addAnOperation();

        DataTransaction t = new DataTransaction(mTransactionId, srcAccOC.getId(), srcAstOC.getId(), dstAccOC.getId(), dstAstOC.getId());
        mDataTransactions.add(t);
        mTransactionId++;
        if (mTransactionId % 100000 == 0)
            LOG.info(String.valueOf(mTransactionId));
    }

    private void UpdateStats() {

        for (int lop = 0; lop < dataConfig.numberOfDLevels; lop++) {

            float accountLevelCount = 0;
            if (mAccountOperationChainsByLevel.containsKey(lop))
                accountLevelCount = mAccountOperationChainsByLevel.get(lop).size() * 1.0f;

            float assetLevelCount = 0;
            if (mAssetsOperationChainsByLevel.containsKey(lop))
                assetLevelCount = mAssetsOperationChainsByLevel.get(lop).size() * 1.0f;

            mAccountLevelsDistribution[lop] = accountLevelCount / totalAccountRecords;
            mAssetLevelsDistribution[lop] = assetLevelCount / totalAssetRecords;
            mOcLevelsDistribution[lop] = (accountLevelCount + assetLevelCount) / (totalAccountRecords + totalAssetRecords);
            mPickAccount[lop] = mAccountLevelsDistribution[lop] < mAssetLevelsDistribution[lop];
        }
//        System.out.println("++++++");
//        System.out.println(mAccountLevelsDistribution[0]
//                + "," + mAccountLevelsDistribution[1]
//                + "," + mAccountLevelsDistribution[2]
//                + "," + mAccountLevelsDistribution[3]);
//        System.out.println(mAssetLevelsDistribution[0]
//                + "," + mAssetLevelsDistribution[1]
//                + "," + mAssetLevelsDistribution[2]
//                + "," + mAssetLevelsDistribution[3]);
//        System.out.println(mOcLevelsDistribution[0]
//                + "," + mOcLevelsDistribution[1]
//                + "," + mOcLevelsDistribution[2]
//                + "," + mOcLevelsDistribution[3]);
//        System.out.println(mPickAccount[0]
//                + "," + mPickAccount[1]
//                + "," + mPickAccount[2]
//                + "," + mPickAccount[3]);
    }

    private DataOperationChain getRandomExistingDestOC(HashMap<Integer, ArrayList<DataOperationChain>> allOcs, DataOperationChain srcOC, DataOperationChain srcAst) {

        ArrayList<DataOperationChain> independentOcs = allOcs.get(0);
        if (independentOcs == null || independentOcs.size() == 0)
            return null;

        DataOperationChain oc = null;
        for (int lop = independentOcs.size() - 1; lop >= 0; lop--) {
            oc = independentOcs.get(lop);
            if (!oc.hasChildren() &&
                    oc != srcOC &&
                    oc != srcAst &&
                    !srcOC.isDependUpon(oc) &&
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

    private void dumpGeneratedDataToFile() {

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

    private void clearDataStructures() {

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
