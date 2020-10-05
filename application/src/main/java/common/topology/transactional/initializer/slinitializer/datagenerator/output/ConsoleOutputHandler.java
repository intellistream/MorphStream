package common.topology.transactional.initializer.slinitializer.datagenerator.output;

import common.topology.transactional.initializer.slinitializer.datagenerator.DataOperationChain;
import common.topology.transactional.initializer.slinitializer.datagenerator.DataTransaction;

import java.util.ArrayList;
import java.util.List;

public class ConsoleOutputHandler implements IOutputHandler {


    @Override
    public void sinkTransactions(List<DataTransaction> dataTransactions) {
        for(DataTransaction dummyTransaction : dataTransactions) {
            System.out.println(dummyTransaction.toString());
        }
    }

    @Override
    public void sinkDependenciesEdges(List<DataOperationChain> allAccountOperationChains, List<DataOperationChain> allAssetOperationChains) {
        printDependencies(allAccountOperationChains);
        printDependencies(allAssetOperationChains);
    }

    private void printDependencies(List<DataOperationChain> operationChains) {
        for(DataOperationChain oc: operationChains) {
            if(!oc.hasDependents()) {
                ArrayList<String> dependencyChains = oc.getDependencyChainInfo();
                for(String dependencyChain: dependencyChains) {
                    System.out.println("\""+dependencyChain+"\",");
                }
            }
        }
    }

    @Override
    public void sinkDependenciesVertices(List<DataOperationChain> allAccountOperationChains, List<DataOperationChain> transactions) {
    }

    @Override
    public void sinkDistributionOfDependencyLevels() {

    }
}
