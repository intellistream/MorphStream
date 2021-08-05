package transaction.scheduler.layered;

import profiler.MeasureTools;
import transaction.TxnProcessingEngine;
import transaction.scheduler.Scheduler;
import transaction.scheduler.layered.struct.OperationChain;
import utils.SOURCE_CONTROL;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;

/**
 * breath-first-search based layered hash scheduler.
 */
@lombok.extern.slf4j.Slf4j
public class BFSLayeredHashScheduler extends Scheduler<OperationChain> {
    public LayeredContext<HashMap<Integer, ArrayDeque<OperationChain>>> context;
    HashMap<Integer, OperationChain> ready_oc = new HashMap<>();
    public BFSLayeredHashScheduler(int tp) {
        context = new LayeredContext<>(tp, HashMap::new);
        for (int threadId = 0; threadId < tp; threadId++) {
            context.layeredOCBucketGlobal.put(threadId, context.createContents());
        }
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
        log.info("localMaxDLevel"+localMaxDLevel);
        return localMaxDLevel;
    }

    private void submit(int threadId, Collection<OperationChain> ocs) {
        context.totalOcsToSchedule[threadId] += ocs.size();
        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = context.layeredOCBucketGlobal.get(threadId);
        buildBucketPerThread(layeredOCBucketThread, ocs);
    }

    @Override
    public void INITIALIZE(int threadId) {
        Collection<TxnProcessingEngine.Holder_in_range> tablesHolderInRange = TxnProcessingEngine.getInstance().getHolder().values();
        for (TxnProcessingEngine.Holder_in_range tableHolderInRange : tablesHolderInRange) {//for each table.
            submit(threadId, tableHolderInRange.rangeMap.get(threadId).holder_v1.values());
        }
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
    }

    @Override
    public void PROCESS(int threadId, long mark_ID) {
        do {
            MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
            OperationChain next = next(threadId);
            MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
            if (next == null) break;
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            TxnProcessingEngine.getInstance().process(threadId, next.getOperations(), mark_ID);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            log.debug("finished process: " + next.toString());
        } while (true);
    }

    /**
     * The following are methods to explore global buckets.
     * It has two different ways: DFS and BFS.
     *
     * @param threadId
     * @return
     */
    public OperationChain BFSearch(int threadId) {
        OperationChain oc = Retrieve(threadId);
        if (oc != null) {
            return oc;//successfully get the next operation chain of the current level.
        } else {
            if (!isFinished(threadId)) {
                while (oc == null) {
                    if (isFinished(threadId))
                        break;
                    context.currentLevel[threadId] += 1;//current level is done, process the next level.
                    oc = Retrieve(threadId);
                    SOURCE_CONTROL.getInstance().waitForOtherThreads();
                }
            }
        }
        return oc;
    }

    private OperationChain next(int threadId) {
        return ready_oc.remove(threadId);// if a null is returned, it means, we are done with level!
    }

    /**
     * This is needed because hash-scheduler can be workload unbalanced.
     *
     * @param threadId
     */
    private void checkFinished(int threadId) {
        if (isFinished(threadId)) {
            SOURCE_CONTROL.getInstance().oneThreadCompleted();
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
        }
    }

    @Override
    public void EXPLORE(int threadId) {
        OperationChain oc = BFSearch(threadId);
        checkFinished(threadId);
        if (oc != null)
            context.scheduledOcsCount[threadId] += 1;
        DISTRIBUTE(oc, threadId);
    }
    @Override
    public void RESET() {

    }
    @Override
    public boolean isFinished(int threadId) {
        return context.finished(threadId);
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param threadId
     * @return
     */
    protected OperationChain Retrieve(int threadId) {
        ArrayDeque<OperationChain> ocs = context.layeredOCBucketGlobal.get(threadId).get(context.currentLevel[threadId]);
        if (ocs == null) {
            System.nanoTime();
        }
        if (ocs.size() > 0)
            return ocs.removeLast();
        else return null;
    }

    @Override
    protected void DISTRIBUTE(OperationChain task, int threadId) {
        ready_oc.put(threadId, task);
    }
}