package common.topology.transactional.initializer.slinitializer.datagenerator;

import common.collections.OsUtils;
import common.topology.transactional.initializer.slinitializer.datagenerator.output.GephiOutputHandler;
import common.topology.transactional.initializer.slinitializer.datagenerator.output.IOutputHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class DataGenerator {

    public static void main(String[] args) {
        String root_path = System.getProperty("user.home") + OsUtils.OS_wrapper("sesame") + OsUtils.OS_wrapper("SYNTH_DATA/");
        new DataGenerator().GenerateData(root_path);
    }

    private ArrayList<DataTransaction> mDataTransactions = new ArrayList<>(DataConfig.tuplesPerBatch);
    private HashMap<Integer, Object> mGeneratedIds = new HashMap<>();

    private HashMap<Integer, ArrayList<DataOperationChain>> accountOperationChainsByLevel = new HashMap<>();
    private HashMap<Integer, ArrayList<DataOperationChain>> assetsOperationChainsByLevel = new HashMap<>();

    private float[] mAccountLevelsDistribution = new float[DataConfig.dependenciesDistributionToLevels.length];
    private float[] mAssetLevelsDistribution = new float[DataConfig.dependenciesDistributionToLevels.length];
    private float[] mOcLevelsDistribution = new float[DataConfig.dependenciesDistributionToLevels.length];
    private boolean[] mPickAccountOrAssets = new boolean[DataConfig.dependenciesDistributionToLevels.length];

    private Random randomGenerator = new Random();
    private IOutputHandler mDataOutputHandler;

    private int[] mPreGeneratedIds;
    private int mNewIdIndex;

    private int totalAccountRecords = 0;
    private int totalAssetRecords = 0;

    private long totalTimeStart = 0;
    private long totalTime = 0;

    private long selectTuplesStart = 0;
    private long selectTuples = 0;

    private long updateDependencyStart = 0;
    private long updateDependency = 0;


    public void GenerateData(String rootPath) {
        mDataOutputHandler = new GephiOutputHandler(rootPath);
        preGeneratedIds();

        for(int batchNumber = 0; batchNumber<DataConfig.totalBatches; batchNumber++) {
            initializeDatStructures();
            for(int tupleNumber = 0; tupleNumber<DataConfig.tuplesPerBatch; tupleNumber++) {

                totalTimeStart = System.nanoTime();
                GenerateTuple();
                totalTime += System.nanoTime() - totalTimeStart;
                UpdateStats();

                if(mDataTransactions.size()%10000 == 0) {

                    float selectTuplesPer = (selectTuples*1.0f)/(totalTime *1.0f)*100.0f;
                    float updateDependencyPer = (updateDependency*1.0f)/(totalTime *1.0f)*100.0f;
                    System.out.println(String.format("Dependency Distribution...select tuple time: %.3f%%, update dependency time: %.3f%%", selectTuplesPer, updateDependencyPer));

                    for(int lop=0;  lop<mOcLevelsDistribution.length; lop++){
                        System.out.print(lop+": "+mOcLevelsDistribution[lop]+"; ");
                    }
                    System.out.println(" ");
                }
            }
            dumpGeneratedDataToFile();
            clearDataStructures();
        }
    }

    private void preGeneratedIds(){

        int totalIdsNeeded = (DataConfig.tuplesPerBatch+1)*4;
        mPreGeneratedIds = new int[totalIdsNeeded];
        for(int index =0; index<totalIdsNeeded; index++) {
            mPreGeneratedIds[index] = getNewId();
        }
        mGeneratedIds.clear();
        mNewIdIndex = 0;
    }

    private void GenerateTuple() {

        DataOperationChain srcAccOC = null;
        DataOperationChain srcAstOC = null;
        DataOperationChain dstAccOC = null;
        DataOperationChain dstAstOC = null;

        boolean srcAcc_dependsUpon_srcAst = false;
        boolean srcAst_dependsUpon_srcAcc = false;

        boolean dstAst_dependsUpon_srcAst = false;
        boolean dstAst_dependsUpon_srcAcc = false;
        boolean dstAst_dependsUpon_dstAcc = false;

        boolean dstAcc_dependsUpon_srcAst = false;
        boolean dstAcc_dependsUpon_srcAcc = false;
        boolean dstAcc_dependsUpon_dstAst = false;

        selectTuplesStart = System.nanoTime();
        if(mOcLevelsDistribution[0] >= DataConfig.dependenciesDistributionToLevels[0]) {

            int selectedLevel = 0;
            for(int lop=1; lop<DataConfig.dependenciesDistributionToLevels.length; lop++) {
                if(mOcLevelsDistribution[lop] < DataConfig.dependenciesDistributionToLevels[lop]) {
                        selectedLevel = lop-1;
                        break;
                }
            }

            if(mPickAccountOrAssets[selectedLevel]) {

                srcAccOC = getRandomExistingOC(selectedLevel, accountOperationChainsByLevel);
                if(srcAccOC==null)
                    srcAccOC = getNewAccountOC();

                srcAstOC = getNewAssetOC();

                dstAccOC = getRandomExistingDestOC(accountOperationChainsByLevel, srcAccOC, srcAstOC);
                if(dstAccOC==null)
                    dstAccOC = getNewAccountOC();

                dstAstOC = getRandomExistingDestOC(assetsOperationChainsByLevel, srcAccOC, srcAstOC);
                if(dstAstOC==null)
                    dstAstOC = getNewAssetOC();

            } else {

                srcAccOC = getNewAccountOC();

                srcAstOC = getRandomExistingOC(selectedLevel, assetsOperationChainsByLevel);
                if(srcAstOC==null)
                    srcAstOC = getNewAssetOC();

                dstAccOC = getRandomExistingDestOC(accountOperationChainsByLevel, srcAccOC, srcAstOC);
                if(dstAccOC==null)
                    dstAccOC = getNewAccountOC();

                dstAstOC = getRandomExistingDestOC(assetsOperationChainsByLevel, srcAccOC, srcAstOC);
                if(dstAstOC==null)
                    dstAstOC = getNewAssetOC();
            }

        }
        else {
            srcAccOC = getNewAccountOC();
            srcAstOC = getNewAssetOC();
            dstAccOC = getNewAccountOC();
            dstAstOC = getNewAssetOC();
        }
        selectTuples += System.nanoTime() - selectTuplesStart;

        dstAcc_dependsUpon_dstAst = dstAccOC.doesDependsUpon(dstAstOC);
        dstAst_dependsUpon_dstAcc = dstAstOC.doesDependsUpon(dstAccOC);

        // register dependencies for srcAssets
        if(srcAccOC.getOperationsCount()>0) {
            srcAst_dependsUpon_srcAcc = srcAstOC.doesDependsUpon(srcAccOC);
            dstAcc_dependsUpon_srcAcc = dstAccOC.doesDependsUpon(srcAccOC);
            dstAst_dependsUpon_srcAcc = dstAstOC.doesDependsUpon(srcAccOC);

            if(!srcAst_dependsUpon_srcAcc) {
                srcAccOC.addDependent(srcAstOC);
                srcAstOC.addDependency(srcAccOC);
                updateDependencyStart = System.nanoTime();
                srcAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if(!dstAcc_dependsUpon_srcAcc && !dstAcc_dependsUpon_dstAst) {
                srcAccOC.addDependent(dstAccOC);
                dstAccOC.addDependency(srcAccOC);
                updateDependencyStart = System.nanoTime();
                dstAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if(!dstAst_dependsUpon_srcAcc && !dstAst_dependsUpon_dstAcc) {
                srcAccOC.addDependent(dstAstOC);
                dstAstOC.addDependency(srcAccOC);
                updateDependencyStart = System.nanoTime();
                dstAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

        } else if(srcAstOC.getOperationsCount()>0) {
            srcAcc_dependsUpon_srcAst = srcAstOC.doesDependsUpon(srcAstOC);
            dstAcc_dependsUpon_srcAst = dstAccOC.doesDependsUpon(srcAstOC);
            dstAst_dependsUpon_srcAst = dstAstOC.doesDependsUpon(srcAstOC);

            if(!srcAcc_dependsUpon_srcAst) {
                srcAstOC.addDependent(srcAccOC);
                srcAccOC.addDependency(srcAstOC);
                updateDependencyStart = System.nanoTime();
                srcAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if(!dstAcc_dependsUpon_srcAst && !dstAcc_dependsUpon_dstAst) {
                srcAstOC.addDependent(dstAccOC);
                dstAccOC.addDependency(srcAstOC);
                updateDependencyStart = System.nanoTime();
                dstAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if(!dstAst_dependsUpon_srcAst && !dstAst_dependsUpon_dstAcc) {
                srcAstOC.addDependent(dstAstOC);
                dstAstOC.addDependency(srcAstOC);
                updateDependencyStart = System.nanoTime();
                dstAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }
        }

        srcAccOC.addAnOperation();
        srcAstOC.addAnOperation();
        dstAccOC.addAnOperation();
        dstAstOC.addAnOperation();

        DataTransaction t = new DataTransaction(mDataTransactions.size(), srcAccOC.getId(), srcAstOC.getId(), dstAccOC.getId(), dstAstOC.getId());
        mDataTransactions.add(t);
        if(mDataTransactions.size()%10000==0)
            System.out.println(mDataTransactions.size());

    }

    private void UpdateStats() {

        for(int lop=0; lop<DataConfig.dependenciesDistributionToLevels.length; lop++) {

            float accountLevelCount = 0;
            if(accountOperationChainsByLevel.containsKey(lop))
                accountLevelCount = accountOperationChainsByLevel.get(lop).size() * 1.0f;

            float assetLevelCount = 0;
            if(assetsOperationChainsByLevel.containsKey(lop))
                assetLevelCount = assetsOperationChainsByLevel.get(lop).size() * 1.0f;

            mAccountLevelsDistribution[lop] = accountLevelCount/totalAccountRecords;
            mAssetLevelsDistribution[lop] = assetLevelCount/totalAssetRecords;
            mOcLevelsDistribution[lop] = (accountLevelCount+assetLevelCount)/(totalAccountRecords+totalAssetRecords);
            mPickAccountOrAssets[lop] = mAccountLevelsDistribution[lop] < mAssetLevelsDistribution[lop];
        }
    }

    private DataOperationChain getRandomExistingDestOC(HashMap<Integer, ArrayList<DataOperationChain>> allOcs, DataOperationChain srcOC, DataOperationChain srcAst) {

        int totalStates = totalAccountRecords+totalAssetRecords;
        if(totalStates>(DataConfig.tuplesPerBatch*DataConfig.generatedTuplesBeforeAddingDependency)) {
            ArrayList<DataOperationChain> independentOcs = allOcs.get(0);
            if(independentOcs==null || independentOcs.size()==0)
                return null;

            DataOperationChain oc = null;
            for(int lop=independentOcs.size()-1; lop>=0; lop--) {
                oc = independentOcs.get(lop);
                if(!oc.hasDependents() &&
                        oc!=srcOC &&
                        oc!=srcAst &&
                        !srcOC.doesDependsUpon(oc)  &&
                        !srcAst.doesDependsUpon(oc))
                    break;
                oc=null;
            }
            return oc;

        } else {
            return null;
        }
    }

    private DataOperationChain getRandomExistingOC(int selectionLevel, HashMap<Integer, ArrayList<DataOperationChain>> ocs) {

        ArrayList<DataOperationChain> selectedLevelFilteredOCs = null;
        if(ocs.containsKey(selectionLevel))
            selectedLevelFilteredOCs = ocs.get(selectionLevel);

        DataOperationChain oc = null;
        if(selectedLevelFilteredOCs!=null && selectedLevelFilteredOCs.size()>0) {
            oc = selectedLevelFilteredOCs.get(randomGenerator.nextInt(selectedLevelFilteredOCs.size()));
        } else {
            oc = null;
        }
        return  oc;
    }

    private void dumpGeneratedDataToFile() {
        System.out.println("Dumping transactions...");
        mDataOutputHandler.sinkTransactions(mDataTransactions);
        System.out.println("Dumping Dependency Edges...");
        mDataOutputHandler.sinkDependenciesEdges(accountOperationChainsByLevel, assetsOperationChainsByLevel);
        System.out.println("Dumping Dependency Vertices...");
        mDataOutputHandler.sinkDependenciesVertices(accountOperationChainsByLevel, assetsOperationChainsByLevel);
        System.out.println("All Done...");
    }

    private DataOperationChain getNewAccountOC() {
        totalAccountRecords++;
        int accId = mPreGeneratedIds[mNewIdIndex];
        DataOperationChain oc = new DataOperationChain("act_"+accId, accountOperationChainsByLevel);
        mNewIdIndex++;
        return  oc;
    }

    private DataOperationChain getNewAssetOC() {
        totalAccountRecords++;
        int astId = mPreGeneratedIds[mNewIdIndex];
        DataOperationChain oc = new DataOperationChain("ast_"+astId, assetsOperationChainsByLevel);
        mNewIdIndex++;
        return  oc;
    }

    // Pre generate them.
    private int getNewId() {
        int id = randomGenerator.nextInt(10*DataConfig.tuplesPerBatch*5);
        while(mGeneratedIds.containsKey(id)) {
            id++;
        }
        mGeneratedIds.put(id, null);
        return id;
    }

    private void initializeDatStructures() {
        mDataTransactions = new ArrayList<>();
    }

    private void clearDataStructures() {

        if(mDataTransactions !=null) {
            mDataTransactions.clear();
            mDataTransactions = null;
        }

    }
}
