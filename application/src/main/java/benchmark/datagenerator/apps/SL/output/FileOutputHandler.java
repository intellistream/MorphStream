package benchmark.datagenerator.apps.SL.output;

import benchmark.datagenerator.apps.SL.OCTxnGenerator.SLDataOperationChain;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.SLDataTransaction;
import common.collections.OsUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FileOutputHandler implements IOutputHandler {

    protected String mRootPath;
    protected String mTransactionsFileName;
    protected String mDependencyEdgesFileName;
    protected String mDependencyVerticesFileName;

    public FileOutputHandler(String rootPath) {
        this(rootPath, null, null, null);
    }

    public FileOutputHandler(String rootPath, String transactionsFileName, String dependencyFileName, String dependencyVerticesFileName) {
        mRootPath = rootPath;

        mTransactionsFileName = transactionsFileName;
        if (mTransactionsFileName == null) {
            mTransactionsFileName = "transactions.txt";
        }

        mDependencyEdgesFileName = dependencyFileName;
        if (mDependencyEdgesFileName == null) {
            mDependencyEdgesFileName = "dependency_edges.csv";
        }

        mDependencyVerticesFileName = dependencyVerticesFileName;
        if (mDependencyVerticesFileName == null) {
            mDependencyVerticesFileName = "dependency_vertices.csv";
        }

    }

    @Override
    public void sinkTransactions(List<SLDataTransaction> dataTransactions) {
        BufferedWriter fileWriter = null;
        try {
            File file = new File(mRootPath + mTransactionsFileName);
            System.out.println(String.format("Transactions path is %s", mRootPath + mTransactionsFileName));
            if (!file.exists())
                file.createNewFile();

            fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));
//            for (int iter = 0; iter < 10; iter++)
//                for (int lop = 0; lop < dataTransactions.size(); lop++)
//                    fileWriter.write(dataTransactions.get(lop).toString(iter, dataTransactions.size()) + "\n");
//            fileWriter.close();
            for (SLDataTransaction dataTransaction : dataTransactions)
                fileWriter.write(dataTransaction.toString() + "\n");
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred while storing transactions.");
            e.printStackTrace();
        }
    }

    @Override
    public void sinkDependenciesEdges(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetOperationChains) {
        FileWriter fileWriter = null;
        try {
            File file = new File(mRootPath + mDependencyEdgesFileName);
            System.out.println(String.format("Edges path is %s", mRootPath + mDependencyEdgesFileName));
            if (!file.exists())
                file.createNewFile();

            fileWriter = new FileWriter(file, true);
            fileWriter.write("source,target\n");

            writeDependencyEdges(allAccountOperationChains, fileWriter);
            writeDependencyEdges(allAssetOperationChains, fileWriter);

            fileWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred while storing dependencies graph.");
            e.printStackTrace();
        }

    }

    private void writeDependencyEdges(HashMap<Integer, ArrayList<SLDataOperationChain>> allOperationChains, FileWriter fileWriter) throws IOException {

        for (ArrayList<SLDataOperationChain> operationChains : allOperationChains.values()) {
            for (SLDataOperationChain oc : operationChains) {
                if (!oc.hasChildren()) {
                    ArrayList<String> dependencyChains = oc.getDependencyChainInfo();
                    for (String dependencyChain : dependencyChains) {
                        fileWriter.write("\"" + dependencyChain + "\",\n");
                    }
                }
            }
        }
    }

    @Override
    public void sinkDependenciesVertices(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetsOperationChains) {
        try {
            File file = new File(mRootPath + mDependencyVerticesFileName);
            System.out.println(String.format("Vertices path is %s", mRootPath + mDependencyVerticesFileName));
            if (!file.exists())
                file.createNewFile();

            File fileGephi = new File(mRootPath + OsUtils.osWrapperPostFix("gephi") + mDependencyVerticesFileName);
            if (!fileGephi.exists()) {
                new File(mRootPath + OsUtils.osWrapperPostFix("gephi")).mkdirs();
                fileGephi.createNewFile();
            }

            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));
            BufferedWriter gephiFileWriter = Files.newBufferedWriter(Paths.get(fileGephi.getPath()));

            gephiFileWriter.write("id,label\n");
            fileWriter.write("label,ops,dLevel\n");

            int vertexId = 0;
            for (ArrayList<SLDataOperationChain> operationChains : allAccountOperationChains.values()) {
                for (SLDataOperationChain chain : operationChains) {
                    fileWriter.write(String.format("%s,%d,%d\n", chain.getStateId(), chain.getOperationsCount(), chain.getDependencyLevel()));
                    gephiFileWriter.write(String.format("%d,%s\n", vertexId, chain.getStateId()));
                    vertexId++;
                }
            }

            for (ArrayList<SLDataOperationChain> operationChains : allAssetsOperationChains.values()) {
                for (SLDataOperationChain chain : operationChains) {
                    fileWriter.write(String.format("%s,%d,%d\n", chain.getStateId(), chain.getOperationsCount(), chain.getDependencyLevel()));
                    gephiFileWriter.write(String.format("%d,%s\n", vertexId, chain.getStateId()));
                    vertexId++;
                }
            }

            fileWriter.close();
            gephiFileWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred while storing transactions.");
            e.printStackTrace();
        }
    }

    @Override
    public void sinkDependenciesVerticesIdsRange(int accountsRange, int assetsRange) {
        try {
            File file = new File(mRootPath + "vertices_ids_range.txt");
            System.out.println(String.format("Vertices ids range path is %s", mRootPath + "vertices_ids_range.txt"));
            if (!file.exists())
                file.createNewFile();

            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));
            fileWriter.write(String.format("accounts=%d,assets=%d", accountsRange, assetsRange));
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred while storing transactions.");
            e.printStackTrace();
        }
    }

    @Override
    public void sinkDistributionOfDependencyLevels() {

    }
}
