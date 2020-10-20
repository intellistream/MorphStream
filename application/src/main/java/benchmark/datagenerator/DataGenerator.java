package benchmark.datagenerator;

import benchmark.datagenerator.output.GephiOutputHandler;
import benchmark.datagenerator.output.IOutputHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

public class DataGenerator {

    private HashMap<Integer, Object> mGeneratedIds = new HashMap<>();
    private Random mRandomGenerator = new Random();
    private int[] mPreGeneratedIds;

    private DatGeneratorConfig dataConfig;

    private ArrayList<DataTransaction> mDataTransactions;
    private HashMap<Integer, ArrayList<DataOperationChain>> mAccountOperationChainsByLevel;
    private HashMap<Integer, ArrayList<DataOperationChain>> mAssetsOperationChainsByLevel;

    private IOutputHandler mDataOutputHandler;

    private float[] mAccountLevelsDistribution;
    private float[] mAssetLevelsDistribution;
    private float[] mOcLevelsDistribution;
    private boolean[] mPickAccountOrAssets;

    private int mNewIdIndex = 0;
    private int mTransactionId = 0;
    private int mTotalTuplesToGenerate;

    private int totalAccountRecords = 0;
    private int totalAssetRecords = 0;

    private long totalTimeStart = 0;
    private long totalTime = 0;

    private long selectTuplesStart = 0;
    private long selectTuples = 0;

    private long updateDependencyStart = 0;
    private long updateDependency = 0;

    public DataGenerator(DatGeneratorConfig dataConfig) {
        this.dataConfig = dataConfig;
        this.mTotalTuplesToGenerate = dataConfig.tuplesPerBatch * dataConfig.totalBatches;
        this.mDataTransactions = new ArrayList<>(mTotalTuplesToGenerate);
        this.mAccountOperationChainsByLevel = new HashMap<>();
        this.mAssetsOperationChainsByLevel = new HashMap<>();
        this.mAccountLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mAssetLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mOcLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mPickAccountOrAssets = new boolean[dataConfig.dependenciesDistributionForLevels.length];
    }

    public void GenerateData() {
        mDataOutputHandler = new GephiOutputHandler(dataConfig.rootPath);
        preGeneratedIds();

        for(int tupleNumber = 0; tupleNumber< mTotalTuplesToGenerate; tupleNumber++) {
            totalTimeStart = System.nanoTime();
            GenerateTuple();
            totalTime += System.nanoTime() - totalTimeStart;
            UpdateStats();

            if(mTransactionId %10000 == 0) {
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
        System.out.println("Date Generation is done...");

    }

    private void preGeneratedIds(){

        int totalIdsNeeded = (mTotalTuplesToGenerate +1)*4;
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
        if(mOcLevelsDistribution[0] >= dataConfig.dependenciesDistributionForLevels[0]) {

            int selectedLevel = 0;
            for(int lop = 1; lop< dataConfig.numberOfDLevels; lop++) {
                if(mOcLevelsDistribution[lop] < dataConfig.dependenciesDistributionForLevels[lop]) {
                        selectedLevel = lop-1;
                        break;
                }
            }

            if(mPickAccountOrAssets[selectedLevel]) {

                srcAccOC = getRandomExistingOC(selectedLevel, mAccountOperationChainsByLevel);
                if(srcAccOC==null)
                    srcAccOC = getNewAccountOC();

                srcAstOC = getNewAssetOC();

                dstAccOC = getRandomExistingDestOC(mAccountOperationChainsByLevel, srcAccOC, srcAstOC);
                if(dstAccOC==null)
                    dstAccOC = getNewAccountOC();

                dstAstOC = getRandomExistingDestOC(mAssetsOperationChainsByLevel, srcAccOC, srcAstOC);
                if(dstAstOC==null)
                    dstAstOC = getNewAssetOC();

            } else {

                srcAccOC = getNewAccountOC();

                srcAstOC = getRandomExistingOC(selectedLevel, mAssetsOperationChainsByLevel);
                if(srcAstOC==null)
                    srcAstOC = getNewAssetOC();

                dstAccOC = getRandomExistingDestOC(mAccountOperationChainsByLevel, srcAccOC, srcAstOC);
                if(dstAccOC==null)
                    dstAccOC = getNewAccountOC();

                dstAstOC = getRandomExistingDestOC(mAssetsOperationChainsByLevel, srcAccOC, srcAstOC);
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

        DataTransaction t = new DataTransaction(mTransactionId, srcAccOC.getId(), srcAstOC.getId(), dstAccOC.getId(), dstAstOC.getId());
        mDataTransactions.add(t);
        mTransactionId++;
        if(mTransactionId %10000==0)
            System.out.println(mTransactionId);

    }

    private void UpdateStats() {

        for(int lop = 0; lop< dataConfig.numberOfDLevels; lop++) {

            float accountLevelCount = 0;
            if(mAccountOperationChainsByLevel.containsKey(lop))
                accountLevelCount = mAccountOperationChainsByLevel.get(lop).size() * 1.0f;

            float assetLevelCount = 0;
            if(mAssetsOperationChainsByLevel.containsKey(lop))
                assetLevelCount = mAssetsOperationChainsByLevel.get(lop).size() * 1.0f;

            mAccountLevelsDistribution[lop] = accountLevelCount/totalAccountRecords;
            mAssetLevelsDistribution[lop] = assetLevelCount/totalAssetRecords;
            mOcLevelsDistribution[lop] = (accountLevelCount+assetLevelCount)/(totalAccountRecords+totalAssetRecords);
            mPickAccountOrAssets[lop] = mAccountLevelsDistribution[lop] < mAssetLevelsDistribution[lop];
        }
    }

    private DataOperationChain getRandomExistingDestOC(HashMap<Integer, ArrayList<DataOperationChain>> allOcs, DataOperationChain srcOC, DataOperationChain srcAst) {

        int totalStates = totalAccountRecords+totalAssetRecords;
        if(totalStates>(mTotalTuplesToGenerate * dataConfig.generatedTuplesBeforeAddingDependency)) {
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
            oc = selectedLevelFilteredOCs.get(mRandomGenerator.nextInt(selectedLevelFilteredOCs.size()));
        } else {
            oc = null;
        }
        return  oc;
    }

    private void dumpGeneratedDataToFile() {
//        if(dataConfig.shufflingActive) {
//            System.out.println(String.format("Shuffling transactions..."));
//            for(int lop=0; lop<dataConfig.totalBatches; lop++)
//                Collections.shuffle(mDataTransactions.subList(lop*dataConfig.tuplesPerBatch, (lop+1)*dataConfig.tuplesPerBatch));
//        }

        System.out.println(String.format("Dumping transactions..."));
        mDataOutputHandler.sinkTransactions(mDataTransactions);

        System.out.println(String.format("Dumping Dependency Edges..."));
        mDataOutputHandler.sinkDependenciesEdges(mAccountOperationChainsByLevel, mAssetsOperationChainsByLevel);

        System.out.println(String.format("Dumping Dependency Vertices..."));
        mDataOutputHandler.sinkDependenciesVertices(mAccountOperationChainsByLevel, mAssetsOperationChainsByLevel);
    }

    private DataOperationChain getNewAccountOC() {
        totalAccountRecords++;
        int accId = mPreGeneratedIds[mNewIdIndex];
        DataOperationChain oc = new DataOperationChain("act_"+accId, mTotalTuplesToGenerate /dataConfig.numberOfDLevels, mAccountOperationChainsByLevel);
        mNewIdIndex++;
        return  oc;
    }

    private DataOperationChain getNewAssetOC() {
        totalAccountRecords++;
        int astId = mPreGeneratedIds[mNewIdIndex];
        DataOperationChain oc = new DataOperationChain("ast_"+astId, mTotalTuplesToGenerate /dataConfig.numberOfDLevels, mAssetsOperationChainsByLevel);
        mNewIdIndex++;
        return  oc;
    }

    // Pre generate them.
    private int getNewId() {
        int id = mRandomGenerator.nextInt(10 * mTotalTuplesToGenerate * 5);
        while(mGeneratedIds.containsKey(id)) {
            id++;
        }
        mGeneratedIds.put(id, null);
        return id;
    }

    private void clearDataStructures() {

        mNewIdIndex = 0;

        if(mDataTransactions !=null) {
            mDataTransactions.clear();
        }
        mDataTransactions = new ArrayList<>();

        if(mAccountOperationChainsByLevel !=null) {
            mAccountOperationChainsByLevel.clear();
        }
        mAccountOperationChainsByLevel = new HashMap<>();

        if(mAssetsOperationChainsByLevel !=null) {
            mAssetsOperationChainsByLevel.clear();
        }
        mAssetsOperationChainsByLevel = new HashMap<>();

        this.mAccountLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mAssetLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mOcLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mPickAccountOrAssets = new boolean[dataConfig.numberOfDLevels];
    }
}
