package benchmark.datagenerator.apps.SL.output;


import benchmark.datagenerator.apps.SL.SLDataOperationChain;
import benchmark.datagenerator.apps.SL.SLDataTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface IOutputHandler {
    void sinkTransactions(List<SLDataTransaction> dataTransactions);

    void sinkDependenciesEdges(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetsOperationChains);

    void sinkDependenciesVertices(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetsOperationChains);

    void sinkDependenciesVerticesIdsRange(int accountsRange, int assetsRange);

    void sinkDistributionOfDependencyLevels();
}
