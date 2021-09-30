package scheduler.impl.layered;

import profiler.MeasureTools;
import scheduler.context.LayeredTPGContext;
import scheduler.impl.OCScheduler;
import scheduler.struct.AbstractOperation;
import scheduler.struct.layered.LayeredOperationChain;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;

public abstract class LayeredScheduler<Context extends LayeredTPGContext<ExecutionUnit, SchedulingUnit>, ExecutionUnit extends AbstractOperation, SchedulingUnit extends LayeredOperationChain<ExecutionUnit>>
        extends OCScheduler<Context, ExecutionUnit, SchedulingUnit> {

    public LayeredScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(Context context) {
        int threadId = context.thisThreadId;
        tpg.constructTPG(context);
        tpg.firstTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        SchedulingUnit next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        if (next != null) {
//            execute(context, next.getOperations(), mark_ID);
            if (executeWithBusyWait(context, next, mark_ID)) {
                MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
                NOTIFY(next, context);
                MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
            }
        } else {
            next = nextFromBusyWaitQueue(context);
            if (next != null) {
                executeWithBusyWait(context, next, mark_ID);
            }
        }
    }

    /**
     * Used by BFSScheduler.
     *
     * @param context
     * @param operation_chain
     * @param mark_ID
     */
    public void execute(Context context, MyList<ExecutionUnit> operation_chain, long mark_ID) {
        for (ExecutionUnit operation : operation_chain) {
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    private SchedulingUnit next(Context context) {
        SchedulingUnit operationChain = context.ready_oc;
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
    public void DISTRIBUTE(SchedulingUnit task, Context context) {
        context.ready_oc = task;
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param context
     * @return
     */
    protected SchedulingUnit Next(Context context) {
        ArrayList<SchedulingUnit> ocs = context.OCSCurrentLayer(); //
        SchedulingUnit oc = null;
        if (ocs != null && context.currentLevelIndex < ocs.size()) {
            oc = ocs.get(context.currentLevelIndex++);
            context.scheduledOPs += oc.getOperations().size();
        }
        return oc;
    }
}
