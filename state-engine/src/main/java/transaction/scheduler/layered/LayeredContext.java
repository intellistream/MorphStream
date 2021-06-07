package transaction.scheduler.layered;

import index.high_scale_lib.ConcurrentHashMap;

import java.util.function.Supplier;
public class LayeredContext<V> {
    public int[] currentLevel;

    //if Hashed: threadID, levelID, listOCs.
    //if RR: levelID, listOCs.
    //if Shared: levelID, listOCs.
    public ConcurrentHashMap<Integer, V> layeredOCBucketGlobal;
    protected Supplier<V> supplier;

    public LayeredContext(int totalThreads, Supplier<V> supplier) {
        layeredOCBucketGlobal = new ConcurrentHashMap<>(); // TODO: make sure this will not cause trouble with multithreaded access.
        currentLevel = new int[totalThreads];
        this.supplier = supplier;
    }
    public V createContents() {
        return supplier.get();
    }
}
