package common.topology.transactional.initializer.slinitializer.datagenerator.output;

import common.topology.transactional.initializer.slinitializer.datagenerator.DataOperationChain;
import common.topology.transactional.initializer.slinitializer.datagenerator.DataTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface IOutputHandler {
    void sinkTransactions(List<DataTransaction> dataTransactions);
    void sinkDependenciesEdges(HashMap<Integer, ArrayList<DataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<DataOperationChain>> allAssetsOperationChains);
    void sinkDependenciesVertices(HashMap<Integer, ArrayList<DataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<DataOperationChain>> allAssetsOperationChains);
    void sinkDistributionOfDependencyLevels();
}
