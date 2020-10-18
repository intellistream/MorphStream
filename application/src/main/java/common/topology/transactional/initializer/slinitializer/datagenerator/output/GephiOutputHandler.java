package common.topology.transactional.initializer.slinitializer.datagenerator.output;

import common.topology.transactional.initializer.slinitializer.datagenerator.DataOperationChain;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class GephiOutputHandler extends FileOutputHandler {


    public GephiOutputHandler(String rootPath) {
        super(rootPath);
    }

    public GephiOutputHandler(String rootPath, String transactionsFileName, String dependenciesEdgesFileName, String dependenciesVerticesFileName){
        super(rootPath, transactionsFileName, dependenciesEdgesFileName, dependenciesVerticesFileName);
    }

    @Override
    public void sinkDependenciesEdges(HashMap<Integer, ArrayList<DataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<DataOperationChain>> allAssetOperationChains) {
        FileWriter fileWriter = null;
        try {
            File file = new File(mRootPath+mDependencyEdgesFileName);
            if (file.exists())
                file.delete();
            file.createNewFile();

            fileWriter = new FileWriter(file);
            fileWriter.write("source,target\n");
            for(ArrayList<DataOperationChain> operationChains: allAccountOperationChains.values()) {
                for(DataOperationChain oc: operationChains)
                    oc.markReadyForTraversal();
            }

            for(ArrayList<DataOperationChain> operationChains: allAssetOperationChains.values()) {
                for(DataOperationChain oc: operationChains)
                    oc.markReadyForTraversal();
            }
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
                ArrayList<String> dependencies = new ArrayList<>();
                oc.registerAllDependenciesToList(dependencies);
                for(String dependency: dependencies) {
                    fileWriter.write(dependency+"\n");
                }
            }
        }
    }

    @Override
    public void sinkDistributionOfDependencyLevels() {

    }
}
