package scheduler.context.op;


import scheduler.signal.op.OnParentUpdatedSignal;
import scheduler.struct.op.Operation;
import scheduler.struct.op.OperationChain;
import stage.Stage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OPSContext extends OPSchedulerContext {
    public HashMap<Integer, ArrayList<Operation>> allocatedLayeredOCBucket;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    public Operation ready_op;//ready operation per thread.
    public ConcurrentLinkedQueue<OnParentUpdatedSignal> layerBuildHelperQueue = new ConcurrentLinkedQueue<>();

    public OPSContext(int thisThreadId, Stage stage) {
        super(thisThreadId, stage);
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
    public boolean finished() {
        assert scheduledOPs <= totalOsToSchedule && currentLevel <= maxLevel;
        return scheduledOPs == totalOsToSchedule && currentLevel == maxLevel;
    }

    @Override
    public OperationChain createTask(String tableName, String pKey) {
        return new OperationChain(tableName, pKey);
    }

    public ArrayList<Operation> OPSCurrentLayer() {
        return allocatedLayeredOCBucket.get(currentLevel);
    }

//    /**
//     * Build buckets with submitted ocs.
//     * Return the local maximal dependency level.
//     *
//     * @param ocs
//     */
//    public void buildBucketPerThread(Collection<OperationChain> ocs) {
//        int localMaxDLevel = 0;
//        int dependencyLevel;
//        for (OperationChain oc : ocs) {
//            if (oc.getOperations().isEmpty()) {
//                continue;
//            }
//            this.totalOsToSchedule += oc.getOperations().size();
//            oc.updateDependencyLevel();
//            dependencyLevel = oc.getDependencyLevel();
//            if (localMaxDLevel < dependencyLevel)
//                localMaxDLevel = dependencyLevel;
//            if (!allocatedLayeredOCBucket.containsKey(dependencyLevel))
//                allocatedLayeredOCBucket.put(dependencyLevel, new ArrayList<>());
//            allocatedLayeredOCBucket.get(dependencyLevel).addAll(oc.getOperations());
//        }
////        if (enable_log) LOG.debug("localMaxDLevel" + localMaxDLevel);
//        this.maxLevel = localMaxDLevel;
//    }

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

        // this procedure is similar to how partition state manager solves the dependencies among operations,
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