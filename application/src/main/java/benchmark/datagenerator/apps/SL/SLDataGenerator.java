package benchmark.datagenerator.apps.SL;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
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

    public SLDataGenerator(DataGeneratorConfig dataConfig) {
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
        selectTuplesStart = System.nanoTime();
        selectOCsForTransaction();
        selectTuples += System.nanoTime() - selectTuplesStart;

        // Step 2: update OCs dependencies graph for future data generation
        updateOCDependencies();

        // Step 3: create txn with the selected OCs, the specific operations are generated inside.
        SLDataTransaction t = new SLDataTransaction(mTransactionId, srcAccOC.getId(), srcAstOC.getId(), dstAccOC.getId(), dstAstOC.getId());
        mDataTransactions.add(t);
        mTransactionId++;
        if (mTransactionId % 100000 == 0)
            LOG.info(String.valueOf(mTransactionId));

        // Step 4: update the statistics such as dependency distribution to guide future data generation
        UpdateStats();
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

    private void selectOCsForTransaction() {
        // try to check whether the level 0 is generated properly i.e. has similar data distribution as expected
        // if smaller, generate new tuple for level 0, which only need to create new ast and acc
        // if bigger, generate higher level tuples.
        if (mOcLevelsDistribution[0] >= dataConfig.dependenciesDistributionForLevels[0]) {
            // in SL, we only select a dependent OC and keep other OCs independent in one field ast/acc
            // the other field might acc/ast might still be dependent, but it is natural.
            selectOCsWithDependency();
        } else {
            createIndependentOCs();
        }
    }

    private void selectOCsWithDependency() {
        int selectedLevel = selectLevelToCreateOC();
        mPId = mRandomGenerator.nextInt(dataConfig.totalThreads);

        // the basic idea here is to select a source acc/ast that is in the selected dependency level
        // and select other three operations without parent and children i.e. totally independeny
        // this makes the txn in the level selectedLevel+1
        if (mPickAccount[selectedLevel]) {
            System.out.println("++++++ pick account as dependency");
            // pick a existing OC of a typical level from account oc chains map
            pickDependentAccOC(selectedLevel);
            createNewSrcAstOC();
        } else {
            System.out.println("++++++ pick asset as dependency");
            createNewSrcAccOC();
            pickDependentAstOC(selectedLevel);
        }
        // TODO: dst acc oc is always null...
        // This part is mainly to pick a independent OC without parent and children.
        // if exists in current OC graph, then pick it, otherwise create a new one.
        pickIndependentDstAccOC();
        // This part is mainly to pick a independent OC without parent and children.
        // NOTE: this independent OC is picked from mAssetsOperationChainsByLevel
        // this means only from asset perspective, the OC is independent
        // but it is possible to be dependent in account
        // if exists in current OC graph, then pick it, otherwise create a new one.
        pickIndependentDstAstOC();
    }

    private void updateOCDependencies() {
        // 8 possible types of data dependencies in SL
        boolean srcAcc_dependsUpon_srcAst;
        boolean srcAst_dependsUpon_srcAcc;

        boolean dstAst_dependsUpon_dstAcc;
        boolean dstAcc_dependsUpon_dstAst;

        boolean dstAst_dependsUpon_srcAst;
        boolean dstAst_dependsUpon_srcAcc;

        boolean dstAcc_dependsUpon_srcAst;
        boolean dstAcc_dependsUpon_srcAcc;


        // All the following operaitons are trying to update the dependency graph that constructed
        // Such that to select the correct level in the future.
        dstAcc_dependsUpon_dstAst = dstAccOC.isDependUpon(dstAstOC);
        dstAst_dependsUpon_dstAcc = dstAstOC.isDependUpon(dstAccOC);
        if (dstAcc_dependsUpon_dstAst || dstAst_dependsUpon_dstAcc)
            System.out.println("++++++ dependency between dst ast&acc: "
                    + dstAcc_dependsUpon_dstAst + " : " + dstAst_dependsUpon_dstAcc);

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
    }

    private void UpdateStats() {

        for (int lop = 0; lop < dataConfig.numberOfDLevels; lop++) {

            float accountLevelCount = 0;
            if (mAccountOperationChainsByLevel.containsKey(lop))
                accountLevelCount = mAccountOperationChainsByLevel.get(lop).size() * 1.0f;

            float assetLevelCount = 0;
            if (mAssetsOperationChainsByLevel.containsKey(lop))
                assetLevelCount = mAssetsOperationChainsByLevel.get(lop).size() * 1.0f;

            // calculate current oc level distribution in terms of account
            mAccountLevelsDistribution[lop] = accountLevelCount / totalAccountRecords;
            // calculate current oc level distribution in terms of asset
            mAssetLevelsDistribution[lop] = assetLevelCount / totalAssetRecords;
            // calculate current oc level distribution on average
            mOcLevelsDistribution[lop] = (accountLevelCount + assetLevelCount) / (totalAccountRecords + totalAssetRecords);
            // check whether pick an account or asset next time in a level, try to keep the dependency be uniform.
            mPickAccount[lop] = mAccountLevelsDistribution[lop] < mAssetLevelsDistribution[lop];
        }
    }

    private int selectLevelToCreateOC() {
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
        return selectedLevel;
    }

    private void createIndependentOCs() {
        // create a txn
        srcAccOC = getNewAccountOC();
        srcAstOC = getNewAssetOC();
        dstAccOC = getNewAccountOC();
        dstAstOC = getNewAssetOC();
    }

    private void createNewSrcAstOC() {
        // generate a new asset OC for processing
        srcAstOC = getNewAssetOC();
        assignOCToThread();
    }

    private void createNewSrcAccOC() {
        // generate a new account OC for processing
        srcAccOC = getNewAccountOC();
        assignOCToThread();
    }

    private void pickDependentAstOC(int selectedLevel) {
        srcAstOC = getRandomExistingOC(selectedLevel, mAssetsOperationChainsByLevel);
        if (srcAstOC == null)
            srcAstOC = getNewAssetOC();
        else
            System.out.println("++++++ pick an existing srcAstOC: " + srcAstOC.getStateId());
        assignOCToThread();
    }

    private void pickDependentAccOC(int selectedLevel) {
        srcAccOC = getRandomExistingOC(selectedLevel, mAccountOperationChainsByLevel);
        if (srcAccOC == null)
            srcAccOC = getNewAccountOC();
        else
            System.out.println("++++++ pick an existing dstAccOC: " + srcAccOC.getStateId());
        assignOCToThread();
    }

    private void pickIndependentDstAstOC() {
        dstAstOC = getExistingIndependentDestOC(mAssetsOperationChainsByLevel, srcAccOC, srcAstOC);
        if (dstAstOC == null)
            dstAstOC = getNewAssetOC();
        else
            System.out.println("++++++ pick an existing dstAstOC: " + dstAstOC.getStateId());
    }

    private void pickIndependentDstAccOC() {
        dstAccOC = getExistingIndependentDestOC(mAccountOperationChainsByLevel, srcAccOC, srcAstOC);
        if (dstAccOC == null)
            dstAccOC = getNewAccountOC();
        else
            System.out.println("++++++ pick an existing dstAccOC: " + dstAccOC.getStateId());
        assignOCToThread();
    }

    private void assignOCToThread() {
        mPId += 1;
        mPId = mPId % dataConfig.totalThreads;
    }

    private SLDataOperationChain getExistingIndependentDestOC(HashMap<Integer, ArrayList<SLDataOperationChain>> allOcs, SLDataOperationChain srcAcc, SLDataOperationChain srcAst) {

        ArrayList<SLDataOperationChain> independentOcs = allOcs.get(0);
        if (independentOcs == null || independentOcs.size() == 0)
            return null;

        SLDataOperationChain oc = null;
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

    private SLDataOperationChain getRandomExistingOC(int selectionLevel, HashMap<Integer, ArrayList<SLDataOperationChain>> ocs) {

        ArrayList<SLDataOperationChain> selectedLevelFilteredOCs = null;
        if (ocs.containsKey(selectionLevel))
            selectedLevelFilteredOCs = ocs.get(selectionLevel);

        SLDataOperationChain oc = null;
        if (selectedLevelFilteredOCs != null && selectedLevelFilteredOCs.size() > 0) {
            oc = selectedLevelFilteredOCs.get(mRandomGenerator.nextInt(selectedLevelFilteredOCs.size()));
        } else {
            oc = null;
        }
        if (oc != null && (oc.getId() % mPartitionOffset) == mPId)
            oc = null;
        return oc;
    }

    private SLDataOperationChain getNewAccountOC() {

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
        SLDataOperationChain oc = new SLDataOperationChain("act_" + id, (10 * mTotalTuplesToGenerate * 5) / dataConfig.numberOfDLevels, mAccountOperationChainsByLevel);

        return oc;
    }

    private SLDataOperationChain getNewAssetOC() {

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
        SLDataOperationChain oc = new SLDataOperationChain("ast_" + id, (10 * mTotalTuplesToGenerate * 5) / dataConfig.numberOfDLevels, mAssetsOperationChainsByLevel);
        return oc;
    }
}
