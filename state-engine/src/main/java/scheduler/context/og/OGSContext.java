package scheduler.context.og;

import scheduler.struct.og.OperationChain;

import java.util.*;

public class OGSContext extends OGSchedulerContext {

    public HashMap<Integer, ArrayList<OperationChain>> allocatedLayeredOCBucket;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    public OperationChain ready_oc;//ready operation chain per thread.

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public OGSContext(int thisThreadId, int totalThreads) {
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
    public OperationChain createTask(String tableName, String pKey, long bid) {
        OperationChain oc = new OperationChain(tableName, pKey, bid);
//        operationChains.add(oc);
        return oc;
    }

    public ArrayList<OperationChain> OCSCurrentLayer() {
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
    public void buildBucketPerThread(Collection<OperationChain> ocs, HashSet<OperationChain> resolvedOC) {
        // TODO: update this logic to the latest logic that we proposed in operation level
        int localMaxDLevel = 0;
        int dependencyLevel;
        for (OperationChain oc : ocs) {
            if (oc.getOperations().isEmpty()) {
                continue;
            }
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

    public void putBusyWaitOCs(HashSet<OperationChain> resolvedOC, int maxLevel) {
        for (OperationChain oc : resolvedOC) {
            if (!allocatedLayeredOCBucket.containsKey(maxLevel))
                allocatedLayeredOCBucket.put(maxLevel, new ArrayList<>());
            allocatedLayeredOCBucket.get(maxLevel).add(oc);
        }
        this.maxLevel = maxLevel;
    }
};
