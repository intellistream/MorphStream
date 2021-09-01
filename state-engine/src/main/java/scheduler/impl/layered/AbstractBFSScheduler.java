package scheduler.impl.layered;

import scheduler.context.BFSLayeredTPGContext;
import scheduler.struct.layered.bfs.BFSOperation;
import scheduler.struct.layered.bfs.BFSOperationChain;
import utils.SOURCE_CONTROL;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class AbstractBFSScheduler<Context extends BFSLayeredTPGContext> extends LayeredScheduler<Context, BFSOperation, BFSOperationChain> {

    public AbstractBFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    private void ProcessedToNextLevel(Context context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
    }

    @Override
    public void EXPLORE(Context context) {
        BFSOperationChain next = Next(context);
        if (next == null && !context.finished()) { //current level is all processed at the current thread.
            while (next == null) {
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                ProcessedToNextLevel(context);
                next = Next(context);
            }
        }
        DISTRIBUTE(next, context);
    }

    @Override
    protected void NOTIFY(BFSOperationChain operationChain, Context context) {
    }

    @Override
    public void TxnSubmitFinished(Context context) {
    }
}