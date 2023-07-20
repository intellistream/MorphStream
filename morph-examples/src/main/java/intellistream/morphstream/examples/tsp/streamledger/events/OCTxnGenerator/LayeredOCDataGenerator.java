package intellistream.morphstream.examples.tsp.streamledger.events.OCTxnGenerator;

import intellistream.morphstream.examples.utils.datagen.DataGenerator;
import intellistream.morphstream.api.InputEvent;
import intellistream.morphstream.examples.tsp.streamledger.events.SLTransferInputEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class LayeredOCDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    private final Random randomGenerator = new Random();
    private final Random randomGeneratorForAccIds = new Random(12345678);
    private final Random randomGeneratorForAstIds = new Random(123456789);
    protected LayeredOCDataGeneratorConfig dataConfig;
    HashMap<Long, Integer> generatedAccountIds = new HashMap<>();
    HashMap<Long, Integer> mGeneratedAssetIds = new HashMap<>();
    SLDataOperationChain srcAccOC = null;
    SLDataOperationChain srcAstOC = null;
    SLDataOperationChain dstAccOC = null;
    SLDataOperationChain dstAstOC = null;
    private int totalAccountRecords = 0;
    private int totalAssetRecords = 0;
    private ArrayList<InputEvent> dataTransactions;
    private HashMap<Integer, ArrayList<SLDataOperationChain>> accountOperationChainsByLevel;
    private HashMap<Integer, ArrayList<SLDataOperationChain>> assetsOperationChainsByLevel;
    private float[] accountLevelsDistribution;
    private float[] assetLevelsDistribution;
    private float[] ocLevelsDistribution;
    private boolean[] pickAccount;
    private int transactionId = 0;
    private long partitionOffset = 0;
    private int partitionId = 0;

    public LayeredOCDataGenerator(LayeredOCDataGeneratorConfig dataConfig) {
        super(dataConfig);
        this.dataConfig = dataConfig;
        this.dataTransactions = new ArrayList<>(nTuples);
        this.accountOperationChainsByLevel = new HashMap<>();
        this.assetsOperationChainsByLevel = new HashMap<>();
        this.accountLevelsDistribution = new float[dataConfig.getDependenciesDistributionForLevels().length];
        this.assetLevelsDistribution = new float[dataConfig.getDependenciesDistributionForLevels().length];
        this.ocLevelsDistribution = new float[dataConfig.getDependenciesDistributionForLevels().length];
        this.pickAccount = new boolean[dataConfig.getDependenciesDistributionForLevels().length];
        this.partitionOffset = dataConfig.getnKeyStates() / dataConfig.getTotalThreads();
    }


    /**
     * generate a set of operations, group them as OC and construct them as OC graph, then create txn from the created OCs.
     * <p>
     * Step 1: select OCs for txn according to the required OCs dependency distribution
     * Step 2: update OCs dependencies graph for future data generation
     * Step 3: create txn with the selected OCs, the specific operations are generated inside.
     * Step 4: update the statistics such as dependency distribution to guide future data generation
     */
    @Override
    protected void generateTuple() {
        // Step 1: select OCs for txn according to the required OCs dependency distribution
        selectOCsForTransaction();

        // Step 2: update OCs dependencies graph for future data generation
        updateOCDependencies();

        // Step 3: create txn with the selected OCs, the specific operations are generated inside.
        InputEvent t = new SLTransferInputEvent(transactionId, srcAccOC.getId(), srcAstOC.getId(), dstAccOC.getId(), dstAstOC.getId());
        dataTransactions.add(t);
        transactionId++;
        System.out.println(transactionId);
        if (transactionId % 100000 == 0) {
            if (enable_log) LOG.info(String.valueOf(transactionId));
            for (int lop = 0; lop < ocLevelsDistribution.length; lop++) {
                System.out.print(lop + ": " + ocLevelsDistribution[lop] + "; ");
            }
            LOG.info(" ");
        }

        // Step 4: update the statistics such as dependency distribution to guide future data generation
        updateStats();
    }

    @Override
    public void dumpGeneratedDataToFile() {
        File versionFile = new File(dataConfig.getRootPath().substring(0, dataConfig.getRootPath().length() - 1)
                + String.format("_%d.txt", dataConfig.getTotalEvents()));
        try {
            versionFile.createNewFile();
            FileWriter fileWriter = new FileWriter(versionFile);
            fileWriter.write(String.format("Total number of threads  : %d\n", dataConfig.getTotalThreads()));
            fileWriter.write(String.format("Total Events      : %d\n", dataConfig.getTotalEvents()));
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (enable_log) LOG.info("Dumping transactions...");
        try {
            dataOutputHandler.sinkEvents(dataTransactions);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clearDataStructures() {
        if (dataTransactions != null) {
            dataTransactions.clear();
        }
        dataTransactions = new ArrayList<>();

        if (accountOperationChainsByLevel != null) {
            accountOperationChainsByLevel.clear();
        }
        accountOperationChainsByLevel = new HashMap<>();

        if (assetsOperationChainsByLevel != null) {
            assetsOperationChainsByLevel.clear();
        }
        assetsOperationChainsByLevel = new HashMap<>();

        this.accountLevelsDistribution = new float[dataConfig.getNumberOfDLevels()];
        this.assetLevelsDistribution = new float[dataConfig.getNumberOfDLevels()];
        this.ocLevelsDistribution = new float[dataConfig.getNumberOfDLevels()];
        this.pickAccount = new boolean[dataConfig.getNumberOfDLevels()];
        // clear the data structure in super class
        super.clearDataStructures();
    }

    private void selectOCsForTransaction() {
        // try to check whether the level 0 is generated properly i.e. has similar data distribution as expected
        // if smaller, generate new tuple for level 0, which only need to create new ast and acc
        // if bigger, generate higher level tuples.
        if (ocLevelsDistribution[0] >= dataConfig.getDependenciesDistributionForLevels()[0]) {
            // in SL, we only select a dependent OC and keep other OCs independent in one field ast/acc
            // the other field might acc/ast might still be dependent, but it is natural.
            selectOCsWithDependency();
        } else {
            createIndependentOCs();
        }
    }

    private void selectOCsWithDependency() {
        int selectedLevel = selectLevelToCreateOC();
        partitionId = randomGenerator.nextInt(dataConfig.getTotalThreads());

        // the basic idea here is to select a source acc/ast that is in the selected dependency level
        // and select other three operations without parent and children i.e. totally independeny
        // this makes the txn in the level selectedLevel+1
        if (pickAccount[selectedLevel]) {
//            System.out.println("++++++ pick account as dependency");
            // pick a existing OC of a typical level from account oc chains map
            pickDependentAccOC(selectedLevel);
            createNewSrcAstOC();
        } else {
//            System.out.println("++++++ pick asset as dependency");
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
        long updateDependencyStart = 0;
        if (srcAccOC.getOperationsCount() > 0) {
            srcAst_dependsUpon_srcAcc = srcAstOC.isDependUpon(srcAccOC);
            dstAcc_dependsUpon_srcAcc = dstAccOC.isDependUpon(srcAccOC);
            dstAst_dependsUpon_srcAcc = dstAstOC.isDependUpon(srcAccOC);

            if (!srcAst_dependsUpon_srcAcc) {
                srcAccOC.addChildren(srcAstOC);
                srcAstOC.addParent(srcAccOC);
                updateDependencyStart = System.nanoTime();
                srcAstOC.updateAllDependencyLevel();
            }

            if (!dstAcc_dependsUpon_srcAcc && !dstAcc_dependsUpon_dstAst) {
                srcAccOC.addChildren(dstAccOC);
                dstAccOC.addParent(srcAccOC);
                updateDependencyStart = System.nanoTime();
                dstAccOC.updateAllDependencyLevel();
            }

            if (!dstAst_dependsUpon_srcAcc && !dstAst_dependsUpon_dstAcc) {
                srcAccOC.addChildren(dstAstOC);
                dstAstOC.addParent(srcAccOC);
                updateDependencyStart = System.nanoTime();
                dstAstOC.updateAllDependencyLevel();
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
            }

            if (!dstAcc_dependsUpon_srcAst && !dstAcc_dependsUpon_dstAst) {
                srcAstOC.addChildren(dstAccOC);
                dstAccOC.addParent(srcAstOC);
                updateDependencyStart = System.nanoTime();
                dstAccOC.updateAllDependencyLevel();
            }

            if (!dstAst_dependsUpon_srcAst && !dstAst_dependsUpon_dstAcc) {
                srcAstOC.addChildren(dstAstOC);
                dstAstOC.addParent(srcAstOC);
                updateDependencyStart = System.nanoTime();
                dstAstOC.updateAllDependencyLevel();
            }
        }

        srcAccOC.addAnOperation();
        srcAstOC.addAnOperation();
        dstAccOC.addAnOperation();
        dstAstOC.addAnOperation();
    }

    private void updateStats() {

        for (int lop = 0; lop < dataConfig.getNumberOfDLevels(); lop++) {

            float accountLevelCount = 0;
            if (accountOperationChainsByLevel.containsKey(lop))
                accountLevelCount = accountOperationChainsByLevel.get(lop).size() * 1.0f;

            float assetLevelCount = 0;
            if (assetsOperationChainsByLevel.containsKey(lop))
                assetLevelCount = assetsOperationChainsByLevel.get(lop).size() * 1.0f;

            // calculate current oc level distribution in terms of account
            accountLevelsDistribution[lop] = accountLevelCount / totalAccountRecords;
            // calculate current oc level distribution in terms of asset
            assetLevelsDistribution[lop] = assetLevelCount / totalAssetRecords;
            // calculate current oc level distribution on average
            ocLevelsDistribution[lop] = (accountLevelCount + assetLevelCount) / (totalAccountRecords + totalAssetRecords);
            // check whether pick an account or asset next time in a level, try to keep the dependency be uniform.
            pickAccount[lop] = accountLevelsDistribution[lop] < assetLevelsDistribution[lop];
        }
    }

    private int selectLevelToCreateOC() {
        // try to select a dependency level for dependency construction
        // simply select the last level of distribution that is bigger as expected
        // e.g. cur distribution: [0.28,0.25,0.25,0.22], expected: [0.25, 0.25, 0.25, 0.25]
        // the selected dependency level is 2.
        int selectedLevel = 0;
        for (int lop = 1; lop < dataConfig.getNumberOfDLevels(); lop++) {
            if (ocLevelsDistribution[lop] < dataConfig.getDependenciesDistributionForLevels()[lop]) {
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
        srcAstOC = getRandomExistingOC(selectedLevel, assetsOperationChainsByLevel);
        if (srcAstOC == null)
            srcAstOC = getNewAssetOC();
//        else
//            System.out.println("++++++ pick an existing srcAstOC: " + srcAstOC.getStateId());
        assignOCToThread();
    }

    private void pickDependentAccOC(int selectedLevel) {
        srcAccOC = getRandomExistingOC(selectedLevel, accountOperationChainsByLevel);
        if (srcAccOC == null)
            srcAccOC = getNewAccountOC();
//        else
//            System.out.println("++++++ pick an existing dstAccOC: " + srcAccOC.getStateId());
        assignOCToThread();
    }

    private void pickIndependentDstAstOC() {
        dstAstOC = getExistingIndependentDestOC(assetsOperationChainsByLevel, srcAccOC, srcAstOC);
        if (dstAstOC == null)
            dstAstOC = getNewAssetOC();
//        else
//            System.out.println("++++++ pick an existing dstAstOC: " + dstAstOC.getStateId());
    }

    private void pickIndependentDstAccOC() {
        dstAccOC = getExistingIndependentDestOC(accountOperationChainsByLevel, srcAccOC, srcAstOC);
        if (dstAccOC == null)
            dstAccOC = getNewAccountOC();
//        else
//            System.out.println("++++++ pick an existing dstAccOC: " + dstAccOC.getStateId());
        assignOCToThread();
    }

    private void assignOCToThread() {
        partitionId += 1;
        partitionId = partitionId % dataConfig.getTotalThreads();
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
                    && (oc.getId() % partitionOffset) == partitionId)
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
            oc = selectedLevelFilteredOCs.get(randomGenerator.nextInt(selectedLevelFilteredOCs.size()));
        } else {
            oc = null;
        }
        if (oc != null && (oc.getId() % partitionOffset) == partitionId)
            oc = null;
        return oc;
    }

    private SLDataOperationChain getNewAccountOC() {
        long id = getUniqueId(randomGeneratorForAccIds, generatedAccountIds, true);
        return new SLDataOperationChain("act_" + id, (nTuples * 5) / dataConfig.getNumberOfDLevels(), accountOperationChainsByLevel);
    }

    private SLDataOperationChain getNewAssetOC() {
        long id = getUniqueId(randomGeneratorForAstIds, mGeneratedAssetIds, false);
        return new SLDataOperationChain("ast_" + id, (nTuples * 5) / dataConfig.getNumberOfDLevels(), assetsOperationChainsByLevel);
    }

    private long getUniqueId(Random randomGeneratorForIds, HashMap<Long, Integer> mGeneratedIds, boolean isAcc) {
        long id = 0;
        int range = (int) partitionOffset;
        if (dataConfig.getIdGenType().equals("uniform")) {
            id = getUniformId(randomGeneratorForIds, isAcc, range);
            while (mGeneratedIds.containsKey(id)) {
//                System.out.println("+++++ conflict");
                id = getUniformId(randomGeneratorForIds, isAcc, range);
            }
        } else if (dataConfig.getIdGenType().equals("normal")) {
            id = getNormalId(randomGeneratorForIds, isAcc, range);
            while (mGeneratedIds.containsKey(id)) {
//                System.out.println("+++++ conflict");
                id = getNormalId(randomGeneratorForIds, isAcc, range);
            }
        }
        mGeneratedIds.put(id, null);

        return id;
    }

    private long getNormalId(Random randomGeneratorForIds, boolean isAcc, int range) {
        long id;
        id = (int) Math.floor(Math.abs(randomGeneratorForIds.nextGaussian() / 3.5) * range) % range;
        id += partitionOffset * partitionId;
//        id *= 10;
        if (isAcc) totalAccountRecords++;
        else totalAssetRecords++;
        return id;
    }

    private long getUniformId(Random randomGeneratorForIds, boolean isAcc, int range) {
        long id;
        id = randomGeneratorForIds.nextInt(range);
        id += partitionOffset * partitionId;
//        id *= 10;
        if (isAcc) {
            totalAccountRecords++;
        } else totalAssetRecords++;
        return id;
    }
}
