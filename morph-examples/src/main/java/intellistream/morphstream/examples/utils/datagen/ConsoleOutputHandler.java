package intellistream.morphstream.examples.utils.datagen;


import intellistream.morphstream.api.InputEvent;
import intellistream.morphstream.examples.tsp.streamledger.events.OCTxnGenerator.SLDataOperationChain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ConsoleOutputHandler implements IOutputHandler {

    @Override
    public void sinkEvents(List<InputEvent> dataTransactions) {
        for (InputEvent dummyTransaction : dataTransactions) {
            System.out.println(dummyTransaction.toString());
        }
    }

    @Override
    public void sinkDependenciesEdges(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetOperationChains) {
        printDependencies(allAccountOperationChains);
        printDependencies(allAssetOperationChains);
    }

    private void printDependencies(HashMap<Integer, ArrayList<SLDataOperationChain>> allOperationChains) {

        for (ArrayList<SLDataOperationChain> operationChains : allOperationChains.values()) {
            for (SLDataOperationChain oc : operationChains) {
                if (!oc.hasChildren()) {
                    ArrayList<String> dependencyChains = oc.getDependencyChainInfo();
                    for (String dependencyChain : dependencyChains) {
                        System.out.println("\"" + dependencyChain + "\",");
                    }
                }
            }
        }
    }

    @Override
    public void sinkDependenciesVertices(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetsOperationChains) {
    }

    @Override
    public void sinkDependenciesVerticesIdsRange(int accountsRange, int assetsRange) {
        System.out.println("Account ids range: " + accountsRange + ", Asset ids range: " + assetsRange);
    }

    @Override
    public void sinkDistributionOfDependencyLevels() {

    }
}
