package intellistream.morphstream.engine.txn.scheduler.impl.og.structured;

import intellistream.morphstream.engine.txn.scheduler.context.og.OGSContext;
import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;

import java.util.Collection;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class AbstractOGDFSScheduler<Context extends OGSContext> extends OGSScheduler<Context> {


    public AbstractOGDFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void EXPLORE(Context context) {
        OperationChain oc = Next(context);
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
    protected void NOTIFY(OperationChain operationChain, Context context) {
//        context.partitionStateManager.onOcExecuted(operationChain);
        operationChain.isExecuted = true; // set operation chain to be executed, which is used for further rollback
        Collection<OperationChain> ocs = operationChain.getChildren();
        for (OperationChain childOC : ocs) {
            childOC.updateDependency();
        }
    }
}
