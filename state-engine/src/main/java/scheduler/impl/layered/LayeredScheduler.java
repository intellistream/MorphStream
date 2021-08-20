package scheduler.impl.layered;

import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.LayeredTPGContext;
import scheduler.impl.Scheduler;
import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;
import scheduler.struct.TaskPrecedenceGraph;
import scheduler.struct.dfs.DFSOperation;
import scheduler.struct.dfs.DFSOperationChain;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.List;

public abstract class LayeredScheduler<Context extends LayeredTPGContext, OC extends OperationChain> extends Scheduler<Context, OC> {

    public TaskPrecedenceGraph tpg; // TPG to be maintained in this global instance.

    public LayeredScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(Context context) {
        int threadId = context.thisThreadId;
        tpg.firstTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        OC next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        if (next != null) {
            execute(context, next.getOperations(), mark_ID);
            NOTIFY(next, context);
        }
    }

    /**
     * Used by BFSScheduler.
     *
     * @param context
     * @param operation_chain
     * @param mark_ID
     */
    public void execute(Context context, MyList<AbstractOperation> operation_chain, long mark_ID) {
        AbstractOperation operation = operation_chain.pollFirst();
        while (operation != null) {
            AbstractOperation finalOperation = operation;
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(finalOperation, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            operation = operation_chain.pollFirst();
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    private OC next(Context context) {
        OC operationChain = (OC) context.ready_oc;
        context.ready_oc = null;
        return operationChain;// if a null is returned, it means, we are done with this level!
    }

    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     */
    @Override
    public void DISTRIBUTE(OC task, Context context) {
        context.ready_oc = task;
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param context
     * @return
     */
    protected OC Next(Context context) {
        ArrayList<OC> ocs = context.OCSCurrentLayer(); //
        OC oc = null;
        if (ocs != null && context.currentLevelIndex < ocs.size()) {
            oc = ocs.get(context.currentLevelIndex++);
            context.scheduledOPs += oc.getOperations().size();
        }
        return oc;
    }
}
