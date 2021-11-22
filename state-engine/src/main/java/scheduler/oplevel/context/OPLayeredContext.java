package scheduler.oplevel.context;


import scheduler.oplevel.signal.op.OnParentUpdatedSignal;
import scheduler.oplevel.struct.Operation;
import scheduler.oplevel.struct.OperationChain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OPLayeredContext extends OPSchedulerContext {
    public HashMap<Integer, ArrayList<Operation>> allocatedLayeredOCBucket;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    public Operation ready_oc;//ready operation chain per thread.
    public ConcurrentLinkedQueue<OnParentUpdatedSignal> layerBuildHelperQueue = new ConcurrentLinkedQueue<>();

    public OPLayeredContext(int thisThreadId) {
        super(thisThreadId);
        this.totalThreads = totalThreads;
        this.allocatedLayeredOCBucket = new HashMap<>();
    }

    @Override
    public void reset() {
        super.reset();
        this.allocatedLayeredOCBucket.clear();
        currentLevel = 0;
        currentLevelIndex = 0;
        maxLevel = 0;
        layerBuildHelperQueue.clear();
    }

    public void redo() {
        super.redo();
        currentLevel = 0;
        currentLevelIndex = 0;
    }

    @Override
    public OperationChain createTask(String tableName, String pKey) {
        return new OperationChain(tableName, pKey);
    }

    public ArrayList<Operation> OPSCurrentLayer() {
        return allocatedLayeredOCBucket.get(currentLevel);
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param ops
     */
    public void buildBucketPerThread(Collection<Operation> ops, Collection<Operation> roots) {
        int localMaxDLevel = 0;
        int dependencyLevel;

        ArrayDeque<Operation> processedOps = new ArrayDeque<>();

        // traverse from roots to update dependency levels
        for (Operation root : roots) {
            updateDependencyLevel(processedOps, root);
        }

        // this procedure is similar to how partition state manager solves the dependnecies among operations,
        // where all dependencies of operations are handled by associated thread
        while (processedOps.size() != ops.size()) {
            OnParentUpdatedSignal signal = layerBuildHelperQueue.poll();
            while (signal != null) {
                Operation operation = signal.getTargetOperation();
                operation.updateDependencies(signal.getType(), signal.getState());
                if (!operation.hasParents()) {
                    updateDependencyLevel(processedOps, operation);
                }
                signal = layerBuildHelperQueue.poll();
            }
        }

        for (Operation op : ops) {
            assert op.hasValidDependencyLevel();
            dependencyLevel = op.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!allocatedLayeredOCBucket.containsKey(dependencyLevel))
                allocatedLayeredOCBucket.put(dependencyLevel, new ArrayList<>());
            allocatedLayeredOCBucket.get(dependencyLevel).add(op);
        }
        this.maxLevel = localMaxDLevel;
    }

    private void updateDependencyLevel(ArrayDeque<Operation> processedOps, Operation operation) {
        operation.calculateDependencyLevelDuringExploration();
        operation.layeredNotifyChildren();
        processedOps.add(operation);
    }
}