package scheduler.context;

import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;
import scheduler.struct.layered.LayeredOperationChain;

import java.util.*;

public abstract class LayeredTPGContext<ExecutionUnit extends AbstractOperation, SchedulingUnit extends LayeredOperationChain<ExecutionUnit>> extends OCSchedulerContext<SchedulingUnit> {

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
    public void redo() {
        super.redo();
        currentLevel = 0;
        currentLevelIndex=0;
    }

    @Override
    public SchedulingUnit createTask(String tableName, String pKey, long bid) {
        throw new UnsupportedOperationException("Unsupported.");
    }

    public ArrayList<SchedulingUnit> OCSCurrentLayer() {
        return allocatedLayeredOCBucket.get(currentLevel);
    }

    @Override
    public boolean finished() {
//        return scheduledOPs == totalOsToSchedule && !needAbortHandling; // not sure whether we need to check this condition.
        return scheduledOPs == totalOsToSchedule && busyWaitQueue.isEmpty();
    }

    public boolean exploreFinished() {
//        return scheduledOPs == totalOsToSchedule && !needAbortHandling; // not sure whether we need to check this condition.
        assert scheduledOPs <= totalOsToSchedule;
        return scheduledOPs == totalOsToSchedule;
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param ocs
     */
    public void buildBucketPerThread(Collection<SchedulingUnit> ocs, HashSet<OperationChain<ExecutionUnit>> resolvedOC) {
        // TODO: update this logic to the latest logic that we proposed in operation level
        int localMaxDLevel = 0;
        int dependencyLevel;
        for (SchedulingUnit oc : ocs) {
            this.totalOsToSchedule += oc.getOperations().size();
            oc.updateDependencyLevel();
            if (resolvedOC.contains(oc)) {
                continue;
            }
            dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!allocatedLayeredOCBucket.containsKey(dependencyLevel))
                allocatedLayeredOCBucket.put(dependencyLevel, new ArrayList<>());
            allocatedLayeredOCBucket.get(dependencyLevel).add(oc);
        }
//        if (enable_log) LOG.debug("localMaxDLevel" + localMaxDLevel);
        this.maxLevel = localMaxDLevel;
    }

    public void putBusyWaitOCs(HashSet<SchedulingUnit> resolvedOC, int maxLevel) {
        for (SchedulingUnit oc : resolvedOC) {
            if (!allocatedLayeredOCBucket.containsKey(maxLevel))
                allocatedLayeredOCBucket.put(maxLevel, new ArrayList<>());
            allocatedLayeredOCBucket.get(maxLevel).add(oc);
        }
        this.maxLevel = maxLevel;
    }
};
