package scheduler.context;

import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;
import scheduler.struct.bfs.BFSOperationChain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public abstract class LayeredTPGContext<ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>> extends SchedulerContext<SchedulingUnit> {

    public HashMap<Integer, ArrayList<SchedulingUnit>> allocatedLayeredOCBucket;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    public SchedulingUnit ready_oc;//ready operation chain per thread.
    public ArrayDeque<ExecutionUnit> abortedOperations;//aborted operations per thread.
    public boolean needAbortHandling = false;//if any operation is aborted during processing.
    public int rollbackLevel = -1; // initialized to 0 if thread not required to be rollbacked.

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public LayeredTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        this.totalThreads = totalThreads;
        this.allocatedLayeredOCBucket = new HashMap<>();
        this.abortedOperations = new ArrayDeque<>();
        requests = new ArrayDeque<>();
    }

    @Override
    protected void reset() {
        currentLevel = 0;
        totalOsToSchedule = 0;
        scheduledOPs = 0;
    }

    @Override
    public SchedulingUnit createTask(String tableName, String pKey) {
        return (SchedulingUnit) new BFSOperationChain(tableName, pKey);
    }

    public ArrayList<SchedulingUnit> OCSCurrentLayer() {
        return allocatedLayeredOCBucket.get(currentLevel);
    }

    @Override
    public boolean finished() {
        return scheduledOPs == totalOsToSchedule && !needAbortHandling;
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
        for (SchedulingUnit oc : ocs) {
            oc.updateDependencyLevel();
            int dependencyLevel = oc.getDependencyLevel();
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
