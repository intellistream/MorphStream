package common.topology.transactional.initializer.slinitializer.datagenerator.output;


import common.topology.transactional.initializer.slinitializer.datagenerator.DataTransaction;
import common.topology.transactional.initializer.slinitializer.datagenerator.DataOperationChain;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
        File rootFolder = new File(mRootPath);
        if(!rootFolder.exists())
            rootFolder.mkdirs();
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

            Collections.shuffle(dataTransactions);

        FileWriter fileWriter = null;
        try {
            File file = new File(mRootPath+mTransactionsFileName);
            if (file.exists())
                file.delete();
            file.createNewFile();

            fileWriter = new FileWriter(file);
            for(int lop = 0; lop< dataTransactions.size(); lop++)
                fileWriter.write (dataTransactions.get(lop).toString()+"\n");
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred while storing transactions.");
            e.printStackTrace();
        }
    }

    @Override
    public void sinkDependenciesEdges(List<DataOperationChain> allAccountOperationChains, List<DataOperationChain> allAssetOperationChains) {
        FileWriter fileWriter = null;
        try {
            File file = new File(mRootPath+mDependencyEdgesFileName);
            if (file.exists())
                file.delete();
            file.createNewFile();

            fileWriter = new FileWriter(file);
            fileWriter.write("source,target\n");

            writeDependencyEdges(allAccountOperationChains, fileWriter);
            writeDependencyEdges(allAssetOperationChains, fileWriter);

            fileWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred while storing dependencies graph.");
            e.printStackTrace();
        }

    }

    private void writeDependencyEdges(List<DataOperationChain> operationChains, FileWriter fileWriter) throws IOException {

        for(DataOperationChain oc: operationChains) {
            if(!oc.hasDependents()) {
                ArrayList<String> dependencyChains = oc.getDependencyChainInfo();
                for(String dependencyChain: dependencyChains) {
                    fileWriter.write("\""+dependencyChain+"\",\n");
                }
            }
        }
    }


    @Override
    public void sinkDependenciesVertices(List<DataOperationChain> allAccountOperationChains, List<DataOperationChain> allAssetsOperationChains) {
         try {
            File file = new File(mRootPath+mDependencyVerticesFileName);
            if (file.exists())
                file.delete();
            file.createNewFile();

             FileWriter fileWriter = null;
             fileWriter = new FileWriter(file);
             fileWriter.write("id,label\n");

             for(DataOperationChain chain: allAccountOperationChains)
                 chain.markReadyForTraversal();

             for(DataOperationChain chain: allAssetsOperationChains)
                 chain.markReadyForTraversal();

             int vertexId = 0;
             for(DataOperationChain chain: allAccountOperationChains) {
                 fileWriter.write(String.format("%d,%s\n",vertexId, chain.getStateId()));
                 vertexId++;
             }

             for(DataOperationChain chain: allAssetsOperationChains) {
                 fileWriter.write(String.format("%d,%s\n",vertexId, chain.getStateId()));
                 vertexId++;
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
