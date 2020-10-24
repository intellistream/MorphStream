package datagenerator;

import common.collections.OsUtils;
import datagenerator.output.GephiOutputHandler;
import datagenerator.output.IOutputHandler;
import sun.security.provider.MD5;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class DataGenerator {

    private HashMap<Integer, Object> mGeneratedIds;
    private Random mRandomGenerator = new Random();
    private int[] mPreGeneratedIds;

    private DataGeneratorConfig dataConfig;

    private ArrayList<DataTransaction> mDataTransactions;
    private HashMap<Integer, ArrayList<DataOperationChain>> mAccountOperationChainsByLevel;
    private HashMap<Integer, ArrayList<DataOperationChain>> mAssetsOperationChainsByLevel;

    private IOutputHandler mDataOutputHandler;

    private float[] mAccountLevelsDistribution;
    private float[] mAssetLevelsDistribution;
    private float[] mOcLevelsDistribution;
    private boolean[] mPickAccountOrAssets;

//    private int mNewIdIndex = 0;
    private int mAccIdIndex = 0;
    private int mAstIdIndex = 0;
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

    public DataGenerator(DataGeneratorConfig dataConfig) {
        this.dataConfig = dataConfig;
        this.mTotalTuplesToGenerate = dataConfig.tuplesPerBatch * dataConfig.totalBatches;
        this.mGeneratedIds = new HashMap<>((mTotalTuplesToGenerate+2)*2);
        this.mDataTransactions = new ArrayList<>(mTotalTuplesToGenerate);
        this.mAccountOperationChainsByLevel = new HashMap<>();
        this.mAssetsOperationChainsByLevel = new HashMap<>();
        this.mAccountLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mAssetLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mOcLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mPickAccountOrAssets = new boolean[dataConfig.dependenciesDistributionForLevels.length];
    }

    public void GenerateData() {

        File file = new File(dataConfig.rootPath);
        if(file.exists()) {
            System.out.println("Data already exists.. skipping data generation...");
            return;
        }

        mDataOutputHandler = new GephiOutputHandler(dataConfig.rootPath);

        preGeneratedIds();
        System.out.println(String.format("Data Generator will dump data at %s.", dataConfig.rootPath));
        for(int tupleNumber = 0; tupleNumber < mTotalTuplesToGenerate; tupleNumber++) {
            totalTimeStart = System.nanoTime();
            GenerateTuple();
            totalTime += System.nanoTime() - totalTimeStart;
            UpdateStats();

            if(mTransactionId %100000 == 0) {
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
        clearDataStructures();
    }

    private void preGeneratedIds() {

        int totalIdsNeeded = (mTotalTuplesToGenerate+1)*2;
        mPreGeneratedIds = new int[totalIdsNeeded];

        FileWriter fileWriter = null;
        try {
            File file = new File(dataConfig.idsPath + String.format("ids_%d.txt", totalIdsNeeded));
            if (!file.exists()) {
                new File(dataConfig.idsPath).mkdirs();
                for(int index =0; index<totalIdsNeeded; index++) {
                    if(index%100000==0)
                        System.out.println(String.format("%d ids generated...", index));
                    mPreGeneratedIds[index] = getNewId();
                }
                System.out.println(String.format("Writing %d ids to file...", totalIdsNeeded));

                file.createNewFile();
                fileWriter = new FileWriter(file, true);
                for(int lop = 0; lop< mPreGeneratedIds.length; lop++)
                    fileWriter.write (mPreGeneratedIds[lop]+"\n");
                fileWriter.close();

                System.out.println(String.format("Done writing ids..."));

            } else {
                System.out.println(String.format("Reading ids from file %s...", String.format("ids_%d.txt", totalIdsNeeded)));
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                for(int index = 0; index< mPreGeneratedIds.length; index++) {
                    mPreGeneratedIds[index] = Integer.parseInt(reader.readLine());
                    if(index%100000==0)
                        System.out.println(String.format("%d ids read...%d", index,mPreGeneratedIds[index]));
                }
                reader.close();
                System.out.println(String.format("Done reading ids..."));
            }
            mGeneratedIds.clear();
            mGeneratedIds = null;
        } catch (IOException e) {
            System.out.println("An error occurred while storing transactions.");
            e.printStackTrace();
        }
//        mNewIdIndex = 0;
        mAccIdIndex = 0;
        mAstIdIndex = 0;
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
        if(mTransactionId %100000==0)
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
                if(independentOcs.size()-lop > 100) {
                    oc = null;
                    break;
                }
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

        File file = new File(dataConfig.rootPath);
        if(file.exists()) {
            System.out.println("Data already exists.. skipping data generation...");
            return;
        }
        file.mkdirs();

        File versionFile = new File(dataConfig.rootPath.substring(0, dataConfig.rootPath.length()-1)+String.format("_%d_%d_%d.txt", dataConfig.tuplesPerBatch,dataConfig.totalBatches,dataConfig.numberOfDLevels));
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

        if(dataConfig.shufflingActive) {
            System.out.println(String.format("Shuffling transactions..."));
            for(int lop=0; lop<dataConfig.totalBatches; lop++)
                Collections.shuffle(mDataTransactions.subList(lop*dataConfig.tuplesPerBatch, (lop+1)*dataConfig.tuplesPerBatch));
        }

        System.out.println(String.format("Dumping transactions..."));
        mDataOutputHandler.sinkTransactions(mDataTransactions);

//        System.out.println(String.format("Dumping Dependency Edges..."));
//        mDataOutputHandler.sinkDependenciesEdges(mAccountOperationChainsByLevel, mAssetsOperationChainsByLevel);

        System.out.println(String.format("Dumping Dependency Vertices..."));
        mDataOutputHandler.sinkDependenciesVertices(mAccountOperationChainsByLevel, mAssetsOperationChainsByLevel);
    }

    private DataOperationChain getNewAccountOC() {
        totalAccountRecords++;
        int accId = mPreGeneratedIds[mAccIdIndex];
        DataOperationChain oc = new DataOperationChain("act_"+accId, mTotalTuplesToGenerate /dataConfig.numberOfDLevels, mAccountOperationChainsByLevel);
        mAccIdIndex++;
        return  oc;
    }

    private DataOperationChain getNewAssetOC() {
        totalAccountRecords++;
        int astId = mPreGeneratedIds[mAstIdIndex];
        DataOperationChain oc = new DataOperationChain("ast_"+astId, mTotalTuplesToGenerate /dataConfig.numberOfDLevels, mAssetsOperationChainsByLevel);
        mAstIdIndex++;
        return  oc;
    }

    // Pre generate them.
    private int getNewId() {
        int id = mRandomGenerator.nextInt(10 * mTotalTuplesToGenerate * 5);
        while(mGeneratedIds.containsKey(id)) {
            id++;
            if(id>=10 * mTotalTuplesToGenerate * 5)
                id = 0;
        }
        mGeneratedIds.put(id, null);
        return id;
    }

    private void clearDataStructures() {

        mAccIdIndex = 0;
        mAstIdIndex = 0;

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
