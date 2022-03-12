package scheduler.impl.og.structured;

import scheduler.context.og.OGDFSContext;
import scheduler.struct.og.structured.dfs.DFSOperation;
import scheduler.struct.og.structured.dfs.DFSOperationChain;

import java.util.Collection;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class AbstractOGDFSScheduler<Context extends OGDFSContext> extends OGSScheduler<Context, DFSOperation, DFSOperationChain> {


    public AbstractOGDFSScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void EXPLORE(Context context) {
        DFSOperationChain oc = Next(context);
        while (oc == null) {
            if (context.exploreFinished())
                break;
            ProcessedToNextLevel(context);
            oc = Next(context);
        }
        while (oc != null && oc.hasParents()) ;
        DISTRIBUTE(oc, context);
    }

    /**
     * notify is handled by state manager of each thread
     *
     * @param operationChain
     * @param context
     */
    @Override
    protected void NOTIFY(DFSOperationChain operationChain, Context context) {
//        context.partitionStateManager.onOcExecuted(operationChain);
        operationChain.isExecuted = true; // set operation chain to be executed, which is used for further rollback
        Collection<DFSOperationChain> ocs = operationChain.getChildren();
        for (DFSOperationChain childOC : ocs) {
            childOC.updateDependency();
        }
    }

    @Override
    public void TxnSubmitFinished(Context context) {
    }
}
