package transaction.scheduler.layered;

import transaction.scheduler.layered.struct.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.Scheduler;
import utils.SOURCE_CONTROL;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;

public abstract class LayeredScheduler  extends Scheduler {
    protected LayeredContext<?> context;

    @Override
    public void reset() {
        context.reset();
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


    /**
     * The following are methods to explore global buckets.
     * It has two different ways: DFS and BFS.
     *
     * @param threadId
     * @return
     */

    public OperationChain BFSearch(int threadId) {
        OperationChain oc = Distribute(threadId);
        if (oc != null) {
            return oc;//successfully get the next operation chain of the current level.
        } else {
            if (!Finished(threadId)) {
                while (oc == null) {
                    if (Finished(threadId))
                        break;
                    context.currentLevel[threadId] += 1;//current level is done, process the next level.
                    oc = Distribute(threadId);
                    MeasureTools.BEGIN_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                    SOURCE_CONTROL.getInstance().waitForOtherThreads();
                    MeasureTools.END_GET_NEXT_BARRIER_TIME_MEASURE(threadId);
                }
            }
        }
        return oc;
    }

    public OperationChain DFSearch(int threadId) {
        OperationChain oc = Distribute(threadId);
        while (oc == null) {
            if (Finished(threadId))
                break;
            context.currentLevel[threadId] += 1;
            oc = Distribute(threadId);
        }
        MeasureTools.BEGIN_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        while (oc != null && oc.hasParents()) ; // Busy-Wait for dependency resolution
        MeasureTools.END_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        return oc;
    }

}
