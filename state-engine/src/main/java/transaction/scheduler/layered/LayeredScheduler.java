package transaction.scheduler.layered;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.IScheduler;
import utils.SOURCE_CONTROL;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
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
    public int buildBucketPerThread(HashMap<Integer, ArrayDeque<OperationChain>> OCBucketThread,
                                    Collection<OperationChain> ocs) {
        int localMaxDLevel = 0;
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!OCBucketThread.containsKey(dependencyLevel))
                OCBucketThread.put(dependencyLevel, new ArrayDeque<>());
            OCBucketThread.get(dependencyLevel).add(oc);
        }
        return localMaxDLevel;
    }

    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = getOC(threadId, currentLevel[threadId]);
        if (oc != null) {
            return oc;//successfully get the next operation chain of the current level.
        } else {
            if (!finishedScheduling(threadId)) {
                while (oc == null) {
                    if (finishedScheduling(threadId))
                        break;
                    currentLevel[threadId] += 1;//current level is done, process the next level.
                    oc = getOC(threadId, currentLevel[threadId]);
                    MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                    SOURCE_CONTROL.getInstance().waitForOtherThreads();
                    MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                }
            }
        }
        return oc;
    }

    protected abstract OperationChain getOC(int threadId, int dLevel);

}
