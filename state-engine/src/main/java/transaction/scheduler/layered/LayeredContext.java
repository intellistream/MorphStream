package transaction.scheduler.layered;

import index.high_scale_lib.ConcurrentHashMap;
import transaction.scheduler.SchedulerContext;

import java.util.function.Supplier;
public class LayeredContext<V> extends SchedulerContext {
    public int[] currentLevel;

    //if Hashed: threadID, levelID, listOCs.
    //if RR: levelID, listOCs.
    //if Shared: levelID, listOCs.
    public ConcurrentHashMap<Integer, V> layeredOCBucketGlobal;
    protected Supplier<V> supplier;
    public int totalThreads;

    public LayeredContext(int totalThreads, Supplier<V> supplier) {
        layeredOCBucketGlobal = new ConcurrentHashMap<>();
        currentLevel = new int[totalThreads];
        this.supplier = supplier;
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
    }
    @Override
    protected boolean finished(int threadId) {
        return false;
    }
}

