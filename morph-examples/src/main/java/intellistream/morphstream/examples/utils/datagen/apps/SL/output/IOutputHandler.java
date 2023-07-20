package intellistream.morphstream.examples.utils.datagen.apps.SL.output;


import intellistream.morphstream.examples.utils.datagen.InputEvent;
import intellistream.morphstream.examples.utils.datagen.apps.SL.OCTxnGenerator.SLDataOperationChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface IOutputHandler {
    void sinkEvents(List<InputEvent> dataTransactions) throws IOException;

    void sinkDependenciesEdges(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetsOperationChains);

    void sinkDependenciesVertices(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetsOperationChains);

    void sinkDependenciesVerticesIdsRange(int accountsRange, int assetsRange);

    void sinkDistributionOfDependencyLevels();
}
