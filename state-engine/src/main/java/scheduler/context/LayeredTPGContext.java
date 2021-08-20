package scheduler.context;

import scheduler.struct.AbstractOperation;
import scheduler.struct.Operation;
import scheduler.struct.OperationChain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;

public class LayeredTPGContext<OP extends AbstractOperation, OC extends OperationChain> extends SchedulerContext {

    public HashMap<Integer, ArrayList<OC>> layeredOCBucketGlobal;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.
    public OC ready_oc;//ready operation chain per thread.
    public ArrayDeque<OP> abortedOperations;//aborted operations per thread.
    public int rollbackLevel;
    public boolean aborted;//if any operation is aborted during processing.

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public LayeredTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        this.totalThreads = totalThreads;
        this.layeredOCBucketGlobal = new HashMap<>();
        this.abortedOperations = new ArrayDeque<>();
        requests = new ArrayDeque<>();
    }

    @Override
    protected void reset() {
        currentLevel = 0;
        totalOsToSchedule = 0;
        scheduledOPs = 0;
    }

    public OC getReady_oc() {
        return ready_oc;
    }

    public ArrayList<OC> OCSCurrentLayer() {
        return layeredOCBucketGlobal.get(currentLevel);
    }

    @Override
    public boolean finished() {
        return scheduledOPs == totalOsToSchedule && !aborted;
    }

};
