package scheduler.impl.og.structured;

import scheduler.context.og.OGBFSContext;
import scheduler.struct.og.structured.bfs.BFSOperation;
import scheduler.struct.og.structured.bfs.BFSOperationChain;
import utils.SOURCE_CONTROL;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class AbstractOGBFSScheduler<Context extends OGBFSContext> extends OGSScheduler<Context, BFSOperation, BFSOperationChain> {

    public AbstractOGBFSScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
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