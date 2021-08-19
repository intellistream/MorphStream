package transaction.scheduler.tpg;

import transaction.scheduler.Request;
import transaction.scheduler.SchedulerContext;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationChain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LayeredTPGContext extends SchedulerContext {

    public final PartitionStateManager partitionStateManager;
    ArrayDeque<Request> requests;

    public HashMap<Integer, ArrayList<OperationChain>> layeredOCBucketGlobal;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    protected int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.
    protected OperationChain ready_oc;//ready operation chain per thread.
    protected ArrayDeque<Operation> abortedOperations;//aborted operations per thread.
    protected int rollbackLevel;
    protected boolean aborted;//if any operation is aborted during processing.

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public LayeredTPGContext(int thisThreadId, int totalThreads) {
        this.thisThreadId = thisThreadId;
        this.totalThreads = totalThreads;
        this.layeredOCBucketGlobal = new HashMap<>();
        this.abortedOperations = new ArrayDeque<>();
        partitionStateManager = new PartitionStateManager();
        requests = new ArrayDeque<>();
    }

    @Override
    protected void reset() {
        currentLevel = 0;
        totalOsToSchedule = 0;
        scheduledOPs = 0;
    }

    @Override
    public void UpdateMapping(String key) {
        //Not required. TODO: cleanup in future.
        partitionStateManager.partition.add(key);
    }

    public ArrayList<OperationChain> BFSearch() {
        return layeredOCBucketGlobal.get(currentLevel);
    }

    @Override
    protected boolean finished() {
        return scheduledOPs == totalOsToSchedule && !aborted;
    }

}
