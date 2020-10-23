package datagenerator.output;


import datagenerator.DataTransaction;
import datagenerator.DataOperationChain;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FileOutputHandler implements IOutputHandler {

    protected String mRootPath;
    protected String mTransactionsFileName;
    protected String mDependencyEdgesFileName;
    protected String mDependencyVerticesFileName;

    public FileOutputHandler(String rootPath){
        this(rootPath, null, null, null);
    }

    public FileOutputHandler(String rootPath, String transactionsFileName, String dependencyFileName, String dependencyVerticesFileName){
        mRootPath = rootPath;

        mTransactionsFileName = transactionsFileName;
        if(mTransactionsFileName==null) {
            mTransactionsFileName = "transactions.txt";
        }

        mDependencyEdgesFileName = dependencyFileName;
        if(mDependencyEdgesFileName ==null) {
            mDependencyEdgesFileName = "dependency_edges.csv";
        }

        mDependencyVerticesFileName = dependencyVerticesFileName;
        if(mDependencyVerticesFileName ==null) {
            mDependencyVerticesFileName = "dependency_vertices.csv";
        }

    }

    @Override
    public void sinkTransactions(List<DataTransaction> dataTransactions) {

        FileWriter fileWriter = null;
        try {
            File file = new File(mRootPath+mTransactionsFileName);
            System.out.println(String.format("Transactions path is %s", mRootPath+mTransactionsFileName));
            if (!file.exists())
                file.createNewFile();

            fileWriter = new FileWriter(file, true);
            for(int lop = 0; lop< dataTransactions.size(); lop++)
                fileWriter.write (dataTransactions.get(lop).toString()+"\n");
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred while storing transactions.");
            e.printStackTrace();
        }
    }

    @Override
    public void sinkDependenciesEdges(HashMap<Integer, ArrayList<DataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<DataOperationChain>> allAssetOperationChains) {
        FileWriter fileWriter = null;
        try {
            File file = new File(mRootPath+mDependencyEdgesFileName);
            System.out.println(String.format("Edges path is %s", mRootPath+mDependencyEdgesFileName));
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

    private void writeDependencyEdges(HashMap<Integer, ArrayList<DataOperationChain>> allOperationChains, FileWriter fileWriter) throws IOException {

        for(ArrayList<DataOperationChain> operationChains: allOperationChains.values()) {
            for(DataOperationChain oc: operationChains) {
                if(!oc.hasDependents()) {
                    ArrayList<String> dependencyChains = oc.getDependencyChainInfo();
                    for(String dependencyChain: dependencyChains) {
                        fileWriter.write("\""+dependencyChain+"\",\n");
                    }
                }
            }
        }
    }


    @Override
    public void sinkDependenciesVertices(HashMap<Integer, ArrayList<DataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<DataOperationChain>> allAssetsOperationChains) {
         try {
             File file = new File(mRootPath+mDependencyVerticesFileName);
             System.out.println(String.format("Vertices path is %s", mRootPath+mDependencyEdgesFileName));
             if (!file.exists())
                 file.createNewFile();

             FileWriter fileWriter = new FileWriter(file, true);
             fileWriter.write("id,label\n");

             int vertexId = 0;
             for(ArrayList<DataOperationChain> operationChains: allAccountOperationChains.values()) {
                 for(DataOperationChain chain: operationChains) {
                     fileWriter.write(String.format("%d,%s\n",vertexId, chain.getStateId()));
                     vertexId++;
                 }
             }

             for(ArrayList<DataOperationChain> operationChains: allAssetsOperationChains.values()) {
                 for(DataOperationChain chain: operationChains) {
                     fileWriter.write(String.format("%d,%s\n",vertexId, chain.getStateId()));
                     vertexId++;
                 }
             }

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
