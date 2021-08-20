package scheduler.impl.layered;

import scheduler.context.LayeredTPGContext;
import scheduler.struct.MetaTypes;
import scheduler.struct.Operation;
import scheduler.struct.OperationChain;

import java.util.Queue;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class DFSScheduler<Context extends LayeredTPGContext> extends LayeredScheduler<Context> {

    public DFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    private void ProcessedToNextLevel(Context context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
    }

    @Override
    public void EXPLORE(Context context) {
        OperationChain oc = Next(context);
        while (oc == null) {
            if (context.finished())
                break;
            ProcessedToNextLevel(context);
            oc = Next(context);
        }
        while (oc != null && oc.hasParents()) ; // Busy-Wait for dependency resolution
        DISTRIBUTE(oc, context);
    }

    /**
     *  notify is handled by state manager of each thread
     *
     * @param operationChain
     * @param context
     */
    @Override
    protected void NOTIFY(OperationChain operationChain, Context context) {
//        context.partitionStateManager.onOcExecuted(operationChain);
        // TODO: operation chain need to be refactored for BFS, DFS, GS
        for (OperationChain childOC : operationChain.getFDChildren()) {
            childOC.updateDependency();
        }
    }
}
