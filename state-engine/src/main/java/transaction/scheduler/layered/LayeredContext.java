package transaction.scheduler.layered;

import index.high_scale_lib.ConcurrentHashMap;
import transaction.Holder_in_range;
import transaction.scheduler.SchedulerContext;
import transaction.scheduler.layered.struct.Operation;
import transaction.scheduler.layered.struct.OperationChain;

import java.util.ArrayDeque;
import java.util.function.Supplier;

public class LayeredContext<V> extends SchedulerContext {
    public ConcurrentHashMap<Integer, V> layeredOCBucketGlobal;//<threadID, HashMap<LevelID, ArrayDeque<OperationChain>>
    protected Supplier<V> supplier;
    private ConcurrentHashMap<String, Holder_in_range> holder_by_stage;//multi table support.
    public int[] currentLevel;
    public int[] currentLevelIndex;
    protected int[] maxLevel;//total number of operations to process per thread.
    protected int[] scheduledOPs;//current number of operations processed per thread.
    protected int[] totalOsToSchedule;//total number of operations to process per thread.
    protected OperationChain[] ready_oc;//ready operation chain per thread.
    protected ArrayDeque<Operation>[] abortedOperations;//aborted operations per thread.
    protected int[] rollbackLevel;
    public int targetRollbackLevel;
    protected volatile boolean aborted;//if any operation is aborted during processing.
    public int totalThreads;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public LayeredContext(int totalThreads, Supplier<V> supplier) {
        this.totalThreads = totalThreads;
        this.supplier = supplier;
        layeredOCBucketGlobal = new ConcurrentHashMap<>();
        currentLevel = new int[totalThreads];
        currentLevelIndex = new int[totalThreads];
        maxLevel = new int[totalThreads];
        scheduledOPs = new int[totalThreads];
        totalOsToSchedule = new int[totalThreads];
        rollbackLevel = new int[totalThreads];

        ready_oc = new OperationChain[totalThreads];
        abortedOperations = new ArrayDeque[totalThreads];
        for (int i = 0; i < totalThreads; i++) {
            abortedOperations[i] = new ArrayDeque<>();
            rollbackLevel[i] = 0;
            currentLevel[i] = 0;
            currentLevelIndex[i] = 0;
        }

        //create holder.
        holder_by_stage = new ConcurrentHashMap<>();
        holder_by_stage.put("accounts", new Holder_in_range(totalThreads));
        holder_by_stage.put("bookEntries", new Holder_in_range(totalThreads));
    }

    public Holder_in_range getHolder(String table_name) {
        return holder_by_stage.get(table_name);
    }

    public ConcurrentHashMap<String, Holder_in_range> getHolder() {
        return holder_by_stage;
    }

    public V createContents() {
        return supplier.get();
    }

    @Override
    protected void reset() {
        layeredOCBucketGlobal.clear();
        for (int threadId = 0; threadId < totalThreads; threadId++) {
            currentLevel[threadId] = 0;
        }
        for (int threadId = 0; threadId < totalThreads; threadId++) {
            totalOsToSchedule[threadId] = 0;
            scheduledOPs[threadId] = 0;
        }
    }

    @Override
    protected boolean finished(int threadId) {
        return scheduledOPs[threadId] == totalOsToSchedule[threadId] && !aborted;
    }
}

