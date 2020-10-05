package common.topology.transactional.initializer.slinitializer.datagenerator.output;

import common.topology.transactional.initializer.slinitializer.datagenerator.DataOperationChain;
import common.topology.transactional.initializer.slinitializer.datagenerator.DataTransaction;

import java.util.List;

public interface IOutputHandler {
    void sinkTransactions(List<DataTransaction> dataTransactions);
    void sinkDependenciesEdges(List<DataOperationChain> allAccountOperationChains, List<DataOperationChain> transactions);
    void sinkDependenciesVertices(List<DataOperationChain> allAccountOperationChains, List<DataOperationChain> transactions);
    void sinkDistributionOfDependencyLevels();
}
