package benchmark.datagenerator.old.output;

import benchmark.datagenerator.old.DataOperationChain;
import benchmark.datagenerator.old.DataTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface IOutputHandler {
    void sinkTransactions(List<DataTransaction> dataTransactions);

    void sinkDependenciesEdges(HashMap<Integer, ArrayList<DataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<DataOperationChain>> allAssetsOperationChains);

    void sinkDependenciesVertices(HashMap<Integer, ArrayList<DataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<DataOperationChain>> allAssetsOperationChains);

    void sinkDependenciesVerticesIdsRange(int accountsRange, int assetsRange);

    void sinkDistributionOfDependencyLevels();
}
