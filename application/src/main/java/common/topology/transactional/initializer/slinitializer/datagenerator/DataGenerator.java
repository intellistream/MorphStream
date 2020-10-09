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
    private ArrayList<Integer> mGeneratedAccountIds = new ArrayList<>();
    private ArrayList<Integer> mGeneratedAssetIds = new ArrayList<>();

    private ArrayList<DataOperationChain> mAllAccountOperationChains = new ArrayList<>();
    private ArrayList<DataOperationChain> mAllAssetOperationChains = new ArrayList<>();

    private float[] mAccountLevelsDistribution = new float[DataConfig.dependenciesDistributionToLevels.length];
    private float[] mAssetLevelsDistribution = new float[DataConfig.dependenciesDistributionToLevels.length];
    private float[] mOcLevelsDistribution = new float[DataConfig.dependenciesDistributionToLevels.length];
    private boolean[] mPickAccountOrAssets = new boolean[DataConfig.dependenciesDistributionToLevels.length];

    private Random randomGenerator = new Random();
    private IOutputHandler mDataOutputHandler;

    public void GenerateData(String rootPath) {
        mDataOutputHandler = new GephiOutputHandler(rootPath);
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

    // Take care of circular dependencies.
    // Take care of percentage of chains with dependencies.
    // TODO: Take care of depth of dependency chains in percentage.

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

                srcAccOC = getRandomExistingOC(selectedLevel, mAllAccountOperationChains);
                if(srcAccOC==null)
                    srcAccOC = getNewAccountOC();

                srcAstOC = getNewAssetOC();

                dstAccOC = getRandomExistingDestOC(mAllAccountOperationChains, srcAccOC, srcAstOC);
                if(dstAccOC==null)
                    dstAccOC = getNewAccountOC();

                dstAstOC = getRandomExistingDestOC(mAllAssetOperationChains, srcAccOC, srcAstOC);
                if(dstAstOC==null)
                    dstAstOC = getNewAssetOC();

            } else {

                srcAccOC = getNewAccountOC();

                srcAstOC = getRandomExistingOC(selectedLevel, mAllAssetOperationChains);
                if(srcAstOC==null)
                    srcAstOC = getNewAssetOC();

                dstAccOC = getRandomExistingDestOC(mAllAccountOperationChains, srcAccOC, srcAstOC);
                if(dstAccOC==null)
                    dstAccOC = getNewAccountOC();

                dstAstOC = getRandomExistingDestOC(mAllAssetOperationChains, srcAccOC, srcAstOC);
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

            getReadyForTraversal();
            if(!dstAstOC.hasInAllDependents(dstAccOC)) {
                dstAccOC.addDependency(srcAccOC);
                srcAccOC.addDependent(dstAccOC);
            }

            getReadyForTraversal();
            if(!dstAccOC.hasInAllDependents(dstAstOC)) {
                dstAstOC.addDependency(srcAccOC);
                srcAccOC.addDependent(dstAstOC);
            }
        }

        // register dependencies for srcAssets
        if(srcAstOC.getOperationsCount()>0) {
            srcAstOC.addDependent(srcAccOC);
            srcAccOC.addDependency(srcAstOC);

            getReadyForTraversal();
            if(!dstAstOC.hasInAllDependents(dstAccOC)) {
                dstAccOC.addDependency(srcAstOC);
                srcAstOC.addDependent(dstAccOC);
            }

            getReadyForTraversal();
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
        srcAccOC.updateAllDependencyLevel();

        DataTransaction t = new DataTransaction(mDataTransactions.size(), srcAccOC.getId(), srcAstOC.getId(), dstAccOC.getId(), dstAstOC.getId());
        mDataTransactions.add(t);
        if(mDataTransactions.size()%1000==0)
            System.out.println(mDataTransactions.size());
    }

    private void getReadyForTraversal() {
        for(DataOperationChain chain: mAllAccountOperationChains)
            chain.markReadyForTraversal();
        for(DataOperationChain chain: mAllAssetOperationChains)
            chain.markReadyForTraversal();
    }

    private void UpdateStats() {

        HashMap<Integer, Integer> accountStatesCountDistribution = new HashMap<>();
        HashMap<Integer, Integer> assetStatesCountDistribution = new HashMap<>();

        for(DataOperationChain oc : mAllAccountOperationChains) {
            int dependencyLevel = oc.getDependencyLevel();
            if(accountStatesCountDistribution.containsKey(dependencyLevel))
                accountStatesCountDistribution.put(dependencyLevel, accountStatesCountDistribution.get(dependencyLevel)+1);
            else
                accountStatesCountDistribution.put(dependencyLevel, 1);
        }

        for(DataOperationChain oc : mAllAssetOperationChains) {
            int dependencyLevel = oc.getDependencyLevel();
            if(assetStatesCountDistribution.containsKey(dependencyLevel))
                assetStatesCountDistribution.put(dependencyLevel, assetStatesCountDistribution.get(dependencyLevel)+1);
            else
                assetStatesCountDistribution.put(dependencyLevel, 1);
        }

        float accountOCCount = mAllAccountOperationChains.size();
        float assetOCCount = mAllAssetOperationChains.size();
        float totalOCCount = accountOCCount+assetOCCount;

        for(int lop=0; lop<DataConfig.dependenciesDistributionToLevels.length; lop++) {

            float accountLevelCount = 0.0f;
            float assetLevelCount = 0.0f;

            if(accountStatesCountDistribution.containsKey(lop))
                accountLevelCount = accountStatesCountDistribution.get(lop)*1.0f;

            if(assetStatesCountDistribution.containsKey(lop))
                assetLevelCount = assetStatesCountDistribution.get(lop)*1.0f;

            mAccountLevelsDistribution[lop] = accountLevelCount/accountOCCount;
            mAssetLevelsDistribution[lop] = assetLevelCount/assetOCCount;
            mOcLevelsDistribution[lop] = (accountLevelCount+assetLevelCount)/totalOCCount;
            mPickAccountOrAssets[lop] = mAccountLevelsDistribution[lop] < mAssetLevelsDistribution[lop];
        }

        if(mDataTransactions.size()%1000 == 0) {
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
        int accId = getNewAccountId();
        DataOperationChain oc = new DataOperationChain("act_"+accId);
        mAllAccountOperationChains.add(oc);
        return  oc;
    }

    private DataOperationChain getRandomExistingDestAccountForOC(DataOperationChain srcOC, DataOperationChain srcAst) {
        DataOperationChain oc = null;
        if(mAllAccountOperationChains.size()>(DataConfig.tuplesPerBatch*DataConfig.generatedTuplesBeforeAddingDependency)) {
            int pos = randomGenerator.nextInt(mAllAccountOperationChains.size());
            oc = mAllAccountOperationChains.get(pos);

            getReadyForTraversal();
            boolean isIndependents = oc.hasInAllDependents(srcOC) || oc.hasInAllDependents(srcAst);
            while(oc==srcOC || isIndependents) {
                pos++;
                if(pos<mAllAccountOperationChains.size()) {
                    oc = mAllAccountOperationChains.get(pos);
                } else {
                    oc = getNewAccountOC();
                    break;
                }
                getReadyForTraversal();
                isIndependents = oc.hasInAllDependents(srcOC) || oc.hasInAllDependents(srcAst);
            }
        } else {
            oc = getNewAccountOC();
        }
        return  oc;
    }

    private DataOperationChain getNewAssetOC() {
        int astId = getNewAssetId();
        DataOperationChain oc = new DataOperationChain("ast_"+astId);
        mAllAssetOperationChains.add(oc);
        return  oc;
    }

    private DataOperationChain getRandomExistingDestAssetForOC(DataOperationChain srcOC, DataOperationChain srcAst) {
        DataOperationChain oc = null;
        if(mAllAssetOperationChains.size()>(DataConfig.tuplesPerBatch*DataConfig.generatedTuplesBeforeAddingDependency)) {
            int pos = randomGenerator.nextInt(mAllAssetOperationChains.size());
            oc = mAllAssetOperationChains.get(pos);

            getReadyForTraversal();
            boolean isIndependents = oc.hasInAllDependents(srcOC) || oc.hasInAllDependents(srcAst);
            while(oc==srcAst || isIndependents) {
                pos++;
                if(pos<mAllAssetOperationChains.size())
                    oc = mAllAssetOperationChains.get(pos);
                else {
                    oc = getNewAssetOC();
                    break;
                }
                getReadyForTraversal();
                isIndependents = oc.hasInAllDependents(srcOC) || oc.hasInAllDependents(srcAst);
            }
        } else {
            oc = getNewAssetOC();
        }
        return  oc;
    }


    private DataOperationChain getRandomExistingDestOC(ArrayList<DataOperationChain> ocs, DataOperationChain srcOC, DataOperationChain srcAst) {

        int totalStates = mAllAssetOperationChains.size()+mAllAccountOperationChains.size();
        if(totalStates>(DataConfig.tuplesPerBatch*DataConfig.generatedTuplesBeforeAddingDependency)) {

            ArrayList<DataOperationChain> selectedLevelFilteredOCs = new ArrayList<>();
            for(DataOperationChain oc: ocs) {
                if(oc.getDependencyLevel()==0 && !oc.hasDependents())
                    selectedLevelFilteredOCs.add(oc);
            }
            if(selectedLevelFilteredOCs.size()==0)
                return null;

            int pos = randomGenerator.nextInt(selectedLevelFilteredOCs.size());
            DataOperationChain oc = selectedLevelFilteredOCs.get(pos);

            getReadyForTraversal();
            boolean isIndependents = oc.hasInAllDependents(srcOC) || oc.hasInAllDependents(srcAst);
            while(oc==srcOC || oc==srcAst || isIndependents) {
                pos++;
                if(pos<selectedLevelFilteredOCs.size()) {
                    oc = selectedLevelFilteredOCs.get(pos);
                    getReadyForTraversal();
                    isIndependents = oc.hasInAllDependents(srcOC) || oc.hasInAllDependents(srcAst);
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


    private DataOperationChain getRandomExistingOC(int selectionLevel, ArrayList<DataOperationChain> ocs) {

        ArrayList<DataOperationChain> selectedLevelFilteredOCs = new ArrayList<>();
        for(DataOperationChain oc: ocs) {
            if(oc.getDependencyLevel()==selectionLevel)
                selectedLevelFilteredOCs.add(oc);
        }

        DataOperationChain oc = null;
        if(selectedLevelFilteredOCs.size()>0) {
            oc = selectedLevelFilteredOCs.get(randomGenerator.nextInt(selectedLevelFilteredOCs.size()));
        } else {
            oc = null;
        }
        return  oc;
    }

    private int getNewAccountId() {
        int id = randomGenerator.nextInt(10*DataConfig.totalBatches*DataConfig.tuplesPerBatch);
        while(mGeneratedAccountIds.contains(id)) {
            id++;
        }
        mGeneratedAccountIds.add(id);
        return id;
    }

    private int getNewAssetId() {
        int id = randomGenerator.nextInt(10*DataConfig.totalBatches*DataConfig.tuplesPerBatch);
        while(mGeneratedAssetIds.contains(id)) {
            id++;
        }
        mGeneratedAssetIds.add(id);
        return id;
    }

    private void initializeDatStructures() {
        mDataTransactions = new ArrayList<>();
        mGeneratedAccountIds = new ArrayList<>();
        mGeneratedAssetIds = new ArrayList<>();

        mAllAccountOperationChains = new ArrayList<>();
        mAllAssetOperationChains = new ArrayList<>();
    }

    private void clearDataStructures() {

        if(mDataTransactions !=null) {
            mDataTransactions.clear();
            mDataTransactions = null;
        }

        if(mGeneratedAccountIds!=null) {
            mGeneratedAccountIds.clear();
            mGeneratedAccountIds = null;
        }

        if(mGeneratedAssetIds!=null) {
            mGeneratedAssetIds.clear();
            mGeneratedAssetIds = null;
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
