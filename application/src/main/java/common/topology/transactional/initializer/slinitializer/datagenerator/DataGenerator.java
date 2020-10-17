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

    private ArrayList<DataTransaction> mDataTransactions = new ArrayList<>();
    private HashMap<Integer, Object> mGeneratedIds = new HashMap<>();

    private ArrayList<DataOperationChain> mAllAccountOperationChains = new ArrayList<>();
    private ArrayList<DataOperationChain> mAllAssetOperationChains = new ArrayList<>();

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

    public void GenerateData(String rootPath) {
        mDataOutputHandler = new GephiOutputHandler(rootPath);
        preGeneratedIds();

        for(int batchNumber = 0; batchNumber<DataConfig.totalBatches; batchNumber++) {
            initializeDatStructures();
            for(int tupleNumber = 0; tupleNumber<DataConfig.tuplesPerBatch; tupleNumber++) {
                GenerateTuple();
                UpdateStats();
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

        // register dependencies for srcAccount
        if(srcAccOC.getOperationsCount()>0) {
            srcAccOC.addDependent(srcAstOC);
            srcAstOC.addDependency(srcAccOC);

            dstAstOC.markDependentsReadyForTraversal();
            if(!dstAstOC.hasInAllDependents(dstAccOC)) {
                dstAccOC.addDependency(srcAccOC);
                srcAccOC.addDependent(dstAccOC);
            }

            dstAccOC.markDependentsReadyForTraversal();
            if(!dstAccOC.hasInAllDependents(dstAstOC)) {
                dstAstOC.addDependency(srcAccOC);
                srcAccOC.addDependent(dstAstOC);
            }
        }

        // register dependencies for srcAssets
        if(srcAstOC.getOperationsCount()>0) {
            srcAstOC.addDependent(srcAccOC);
            srcAccOC.addDependency(srcAstOC);

            dstAstOC.markDependentsReadyForTraversal();
            if(!dstAstOC.hasInAllDependents(dstAccOC)) {
                dstAccOC.addDependency(srcAstOC);
                srcAstOC.addDependent(dstAccOC);
            }

            dstAccOC.markDependentsReadyForTraversal();
            if(!dstAccOC.hasInAllDependents(dstAstOC)) {
                dstAstOC.addDependency(srcAstOC);
                srcAstOC.addDependent(dstAstOC);
            }
        }

        // register operations to
        srcAccOC.addAnOperation();
        srcAstOC.addAnOperation();
        dstAccOC.addAnOperation();
        dstAstOC.addAnOperation();

        srcAccOC.markAllDependencyLevelsDirty();
        srcAstOC.markAllDependencyLevelsDirty();
        srcAccOC.updateAllDependencyLevel();
        srcAstOC.updateAllDependencyLevel();

        DataTransaction t = new DataTransaction(mDataTransactions.size(), srcAccOC.getId(), srcAstOC.getId(), dstAccOC.getId(), dstAstOC.getId());
        mDataTransactions.add(t);
        if(mDataTransactions.size()%10000==0)
            System.out.println(mDataTransactions.size());
    }

    private void getReadyForTraversal() {
        for(DataOperationChain chain: mAllAccountOperationChains)
            chain.markReadyForTraversal();
        for(DataOperationChain chain: mAllAssetOperationChains)
            chain.markReadyForTraversal();
    }

    private void UpdateStats() {

        float accountOCCount = mAllAccountOperationChains.size();
        float assetOCCount = mAllAssetOperationChains.size();
        float totalOCCount = accountOCCount+assetOCCount;

        for(int lop=0; lop<DataConfig.dependenciesDistributionToLevels.length; lop++) {


            float accountLevelCount = 0;
            if(accountOperationChainsByLevel.containsKey(lop))
                accountLevelCount = accountOperationChainsByLevel.get(lop).size() * 1.0f;

            float assetLevelCount = 0;
            if(assetsOperationChainsByLevel.containsKey(lop))
                assetLevelCount = assetsOperationChainsByLevel.get(lop).size() * 1.0f;

            mAccountLevelsDistribution[lop] = accountLevelCount/accountOCCount;
            mAssetLevelsDistribution[lop] = assetLevelCount/assetOCCount;
            mOcLevelsDistribution[lop] = (accountLevelCount+assetLevelCount)/totalOCCount;
            mPickAccountOrAssets[lop] = mAccountLevelsDistribution[lop] < mAssetLevelsDistribution[lop];
        }

        if(mDataTransactions.size()%10000 == 0) {
            System.out.println("Dependency Distribution...");
            for(int lop=0;  lop<mOcLevelsDistribution.length; lop++){
                System.out.print(lop+": "+mOcLevelsDistribution[lop]+"; ");
            }
            System.out.println(" ");
        }
    }

    private void dumpGeneratedDataToFile() {
        System.out.println("Dumping transactions...");
        mDataOutputHandler.sinkTransactions(mDataTransactions);
        System.out.println("Dumping Dependency Edges...");
        mDataOutputHandler.sinkDependenciesEdges(mAllAccountOperationChains, mAllAssetOperationChains);
        System.out.println("Dumping Dependency Vertices...");
        mDataOutputHandler.sinkDependenciesVertices(mAllAccountOperationChains, mAllAssetOperationChains);
        System.out.println("All Done...");
    }


    private DataOperationChain getNewAccountOC() {
        int accId = mPreGeneratedIds[mNewIdIndex];
        DataOperationChain oc = new DataOperationChain("act_"+accId, accountOperationChainsByLevel);
        mAllAccountOperationChains.add(oc);
        mNewIdIndex++;
        return  oc;
    }

    private DataOperationChain getNewAssetOC() {
        int astId = mPreGeneratedIds[mNewIdIndex];
        DataOperationChain oc = new DataOperationChain("ast_"+astId, assetsOperationChainsByLevel);
        mAllAssetOperationChains.add(oc);
        mNewIdIndex++;
        return  oc;
    }

    private DataOperationChain getRandomExistingDestOC(HashMap<Integer, ArrayList<DataOperationChain>> allOcs, DataOperationChain srcOC, DataOperationChain srcAst) {

        int totalStates = mAllAssetOperationChains.size()+mAllAccountOperationChains.size();
        if(totalStates>(DataConfig.tuplesPerBatch*DataConfig.generatedTuplesBeforeAddingDependency)) {

            ArrayList<DataOperationChain> independentOcs = null;
            if(allOcs.containsKey(0))
                independentOcs = allOcs.get(0);
            else
                return null;

            ArrayList<DataOperationChain> selectedLevelFilteredOCs = new ArrayList<>();
            for(DataOperationChain oc: independentOcs) {
                if(!oc.hasDependents())
                    selectedLevelFilteredOCs.add(oc);
            }

            if(selectedLevelFilteredOCs.size()==0)
                return null;

            int pos = randomGenerator.nextInt(selectedLevelFilteredOCs.size());
            DataOperationChain oc = selectedLevelFilteredOCs.get(pos);

            boolean isIndependents = false;
            oc.markDependentsReadyForTraversal();
            isIndependents = oc.hasInAllDependents(srcOC);
            oc.markDependentsReadyForTraversal();
            isIndependents |= oc.hasInAllDependents(srcAst);

            while(oc==srcOC || oc==srcAst || isIndependents) {
                pos++;
                if(pos<selectedLevelFilteredOCs.size()) {
                    oc = selectedLevelFilteredOCs.get(pos);

                    oc.markDependentsReadyForTraversal();
                    isIndependents = oc.hasInAllDependents(srcOC);
                    oc.markDependentsReadyForTraversal();
                    isIndependents |= oc.hasInAllDependents(srcAst);
                } else {
                    oc = null;
                    break;
                }
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


    // Pre generate them.
    // Each thread can choose an id like we select transactions for each thread in TStream.
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

        mAllAccountOperationChains = new ArrayList<>();
        mAllAssetOperationChains = new ArrayList<>();
    }

    private void clearDataStructures() {

        if(mDataTransactions !=null) {
            mDataTransactions.clear();
            mDataTransactions = null;
        }


        if(mAllAccountOperationChains!=null) {
            mAllAccountOperationChains.clear();
            mAllAccountOperationChains = null;
        }

        if(mAllAssetOperationChains!=null) {
            mAllAssetOperationChains.clear();
            mAllAssetOperationChains = null;
        }
    }

}
