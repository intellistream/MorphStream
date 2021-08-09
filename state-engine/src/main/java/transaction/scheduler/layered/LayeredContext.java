package transaction.scheduler.layered;

import transaction.scheduler.SchedulerContext;
import transaction.scheduler.layered.struct.Operation;
import transaction.scheduler.layered.struct.OperationChain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;

public class LayeredContext extends SchedulerContext {
    public HashMap<Integer, ArrayList<OperationChain>> layeredOCBucketGlobal;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    protected int maxLevel;//total number of operations to process per thread.
    protected int scheduledOPs;//current number of operations processed per thread.
    protected int totalOsToSchedule;//total number of operations to process per thread.
    protected OperationChain ready_oc;//ready operation chain per thread.
    protected ArrayDeque<Operation> abortedOperations;//aborted operations per thread.
    protected int rollbackLevel;
    protected boolean aborted;//if any operation is aborted during processing.

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public LayeredContext(int thisThreadId, int totalThreads) {
        this.thisThreadId = thisThreadId;
        this.totalThreads = totalThreads;
        this.layeredOCBucketGlobal = new HashMap<>();
        this.abortedOperations = new ArrayDeque<>();
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
    }

    public ArrayList<OperationChain> BFSearch() {
        return layeredOCBucketGlobal.get(currentLevel);
    }

    @Override
    protected boolean finished() {
        return scheduledOPs == totalOsToSchedule && !aborted;
    }

}

