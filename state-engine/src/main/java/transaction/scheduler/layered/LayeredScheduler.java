package transaction.scheduler.layered;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.IScheduler;
import utils.SOURCE_CONTROL;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;

public abstract class LayeredScheduler implements IScheduler {
    protected LayeredContext<?> context;

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

    public OperationChain BFSearch(int threadId) {
        OperationChain oc = getOC(threadId, context.currentLevel[threadId]);
        if (oc != null) {
            return oc;//successfully get the next operation chain of the current level.
        } else {
            if (!finishedScheduling(threadId)) {
                while (oc == null) {
                    if (finishedScheduling(threadId))
                        break;
                    context.currentLevel[threadId] += 1;//current level is done, process the next level.
                    oc = getOC(threadId, context.currentLevel[threadId]);
                    MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                    SOURCE_CONTROL.getInstance().waitForOtherThreads();
                    MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                }
            }
        }
        return oc;
    }

    public OperationChain DFSearch(int threadId) {
        OperationChain oc = getOC(threadId, context.currentLevel[threadId]);
        while (oc == null) {
            if (finishedScheduling(threadId))
                break;
            context.currentLevel[threadId] += 1;
            oc = getOC(threadId, context.currentLevel[threadId]);
        }
        MeasureTools.BEGIN_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        while (oc != null && oc.hasParents()) ; // Busy-Wait for dependency resolution
        MeasureTools.END_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        return oc;
    }

    protected abstract OperationChain getOC(int threadId, int dLevel);

}
