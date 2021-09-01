package scheduler.context;

import scheduler.struct.AbstractOperation;
import scheduler.struct.layered.LayeredOperationChain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public abstract class LayeredTPGContext<ExecutionUnit extends AbstractOperation, SchedulingUnit extends LayeredOperationChain<ExecutionUnit>> extends SchedulerContext<SchedulingUnit> {

    public HashMap<Integer, ArrayList<SchedulingUnit>> allocatedLayeredOCBucket;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    public SchedulingUnit ready_oc;//ready operation chain per thread.

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public LayeredTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        this.totalThreads = totalThreads;
        this.allocatedLayeredOCBucket = new HashMap<>();
        requests = new ArrayDeque<>();
    }

    @Override
    public void reset() {
        super.reset();
        allocatedLayeredOCBucket.clear();
        currentLevel = 0;
        currentLevelIndex=0;
        maxLevel=0;
    }

    @Override
    public SchedulingUnit createTask(String tableName, String pKey) {
        throw new UnsupportedOperationException("Unsupported.");
    }

    public ArrayList<SchedulingUnit> OCSCurrentLayer() {
        return allocatedLayeredOCBucket.get(currentLevel);
    }

    @Override
    public boolean finished() {
//        return scheduledOPs == totalOsToSchedule && !needAbortHandling; // not sure whether we need to check this condition.
        return scheduledOPs == totalOsToSchedule;
    }


    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param ocs
     * @return
     */
    public void buildBucketPerThread(Collection<SchedulingUnit> ocs) {
        int localMaxDLevel = 0;
        int dependencyLevel;
        for (SchedulingUnit oc : ocs) {
            oc.updateDependencyLevel();
            dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!allocatedLayeredOCBucket.containsKey(dependencyLevel))
                allocatedLayeredOCBucket.put(dependencyLevel, new ArrayList<>());
            allocatedLayeredOCBucket.get(dependencyLevel).add(oc);
        }
//        if (enable_log) LOG.debug("localMaxDLevel" + localMaxDLevel);
        this.maxLevel = localMaxDLevel;
//        return localMaxDLevel;
    }
};
