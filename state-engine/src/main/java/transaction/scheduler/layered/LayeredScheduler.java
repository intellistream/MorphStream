package transaction.scheduler.layered;

import common.OperationChain;
import transaction.scheduler.IScheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class LayeredScheduler<V> implements IScheduler {

    //if Hashed: threadID, levelID, listOCs.
    //if RR: levelID, listOCs.
    //if Shared: levelID, listOCs.
    protected ConcurrentHashMap<Integer, V> layeredOCBucketGlobal;
    protected int[] currentLevel;

    public LayeredScheduler(int totalThreads) {
        layeredOCBucketGlobal = new ConcurrentHashMap(); // TODO: make sure this will not cause trouble with multithreaded access.
        currentLevel = new int[totalThreads];
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param OCBucketThread
     * @param ocs
     * @return
     */
    public int buildBucketPerThread(HashMap<Integer, List<OperationChain>> OCBucketThread,
                                    Collection<OperationChain> ocs) {
        int localMaxDLevel = 0;
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!OCBucketThread.containsKey(dependencyLevel))
                OCBucketThread.put(dependencyLevel, new ArrayList<>());
            OCBucketThread.get(dependencyLevel).add(oc);
        }
        return localMaxDLevel;
    }
}
