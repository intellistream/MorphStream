package scheduler.impl.layered;

import scheduler.context.LayeredTPGContext;
import scheduler.struct.OperationChain;
import utils.SOURCE_CONTROL;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class BFSScheduler<Context extends LayeredTPGContext> extends LayeredScheduler<Context> {

    public BFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    private void ProcessedToNextLevel(Context context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
    }

    @Override
    public void EXPLORE(Context context) {
        OperationChain next = Next(context);
        if (next == null && !context.finished()) {//current level is all processed at the current thread.
            while (next == null) {
                ProcessedToNextLevel(context);
                next = Next(context);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                //all threads come to the current level.
            }
        }
        DISTRIBUTE(next, context);
    }
}
