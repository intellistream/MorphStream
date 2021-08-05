package benchmark.datagenerator.apps.SL.output;


import benchmark.datagenerator.apps.SL.OCScheduler.SLDataOperationChain;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class GephiOutputHandler extends FileOutputHandler {

    public GephiOutputHandler(String rootPath) {
        super(rootPath);
    }

    public GephiOutputHandler(String rootPath, String transactionsFileName, String dependenciesEdgesFileName, String dependenciesVerticesFileName) {
        super(rootPath, transactionsFileName, dependenciesEdgesFileName, dependenciesVerticesFileName);
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
            for (ArrayList<SLDataOperationChain> operationChains : allAccountOperationChains.values()) {
                for (SLDataOperationChain oc : operationChains)
                    oc.markReadyForTraversal();
            }

            for (ArrayList<SLDataOperationChain> operationChains : allAssetOperationChains.values()) {
                for (SLDataOperationChain oc : operationChains)
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

    private void writeDependencyEdges(HashMap<Integer, ArrayList<SLDataOperationChain>> allOperationChains, FileWriter fileWriter) throws IOException {

        for (ArrayList<SLDataOperationChain> operationChains : allOperationChains.values()) {
            for (SLDataOperationChain oc : operationChains) {
                ArrayList<String> dependencies = new ArrayList<>();
                oc.registerAllDependenciesToList(dependencies);
                for (String dependency : dependencies) {
                    fileWriter.write(dependency + "\n");
                }
            }
        }
    }

    @Override
    public void sinkDistributionOfDependencyLevels() {

    }
}
