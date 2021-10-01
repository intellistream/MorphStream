package scheduler.oplevel.context;


import scheduler.oplevel.struct.Operation;
import scheduler.oplevel.struct.OperationChain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class OPLayeredContext extends OPSchedulerContext {
    public HashMap<Integer, ArrayList<Operation>> allocatedLayeredOCBucket;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    public Operation ready_oc;//ready operation chain per thread.

    public OPLayeredContext(int thisThreadId) {
        super(thisThreadId);
        this.totalThreads = totalThreads;
        this.allocatedLayeredOCBucket = new HashMap<>();
    }

    @Override
    public void reset() {
        super.reset();
        this.allocatedLayeredOCBucket.clear();
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
     * @return
     */
    public int buildBucketPerThread(Collection<Operation> ops) {
        int localMaxDLevel = 0;
        int dependencyLevel;
        for (Operation op : ops) {
            op.updateDependencyLevel();
            dependencyLevel = op.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!allocatedLayeredOCBucket.containsKey(dependencyLevel))
                allocatedLayeredOCBucket.put(dependencyLevel, new ArrayList<>());
            allocatedLayeredOCBucket.get(dependencyLevel).add(op);
        }
        this.maxLevel = localMaxDLevel;
        return localMaxDLevel;
    }
}