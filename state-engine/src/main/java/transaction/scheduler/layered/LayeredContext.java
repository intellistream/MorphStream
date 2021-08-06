package transaction.scheduler.layered;

import index.high_scale_lib.ConcurrentHashMap;
import transaction.Holder_in_range;
import transaction.scheduler.SchedulerContext;

import java.util.function.Supplier;
public class LayeredContext<V> extends SchedulerContext {
    private ConcurrentHashMap<String, Holder_in_range> holder_by_stage;//multi table support.

    public int[] currentLevel;
    protected int[] scheduledOcsCount;//current number of operation chains processed per thread.
    protected int[] totalOcsToSchedule;//total number of operation chains to process per thread.
    protected int[] totalOsToSchedule;//total number of operations to process per thread.

    public ConcurrentHashMap<Integer, V> layeredOCBucketGlobal;//<threadID, HashMap<LevelID, ArrayDeque<OperationChain>>
    protected Supplier<V> supplier;
    public int totalThreads;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public LayeredContext(int totalThreads, Supplier<V> supplier) {
        this.totalThreads = totalThreads;
        this.supplier = supplier;
        layeredOCBucketGlobal = new ConcurrentHashMap<>();
        currentLevel = new int[totalThreads];
        scheduledOcsCount = new int[totalThreads];
        totalOcsToSchedule = new int[totalThreads];
        totalOsToSchedule = new int[totalThreads];

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
            totalOcsToSchedule[threadId] = 0;
            scheduledOcsCount[threadId] = 0;
        }
    }
    @Override
    protected boolean finished(int threadId) {
        return scheduledOcsCount[threadId] == totalOcsToSchedule[threadId];
    }
}

