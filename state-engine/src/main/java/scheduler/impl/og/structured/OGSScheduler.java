package scheduler.impl.og.structured;

import scheduler.context.og.OGSContext;
import scheduler.impl.og.OGScheduler;
import scheduler.struct.og.AbstractOperation;
import scheduler.struct.og.structured.SOperationChain;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;

public abstract class OGSScheduler<Context extends OGSContext<ExecutionUnit, SchedulingUnit>, ExecutionUnit extends AbstractOperation, SchedulingUnit extends SOperationChain<ExecutionUnit>>
        extends OGScheduler<Context, ExecutionUnit, SchedulingUnit> {

    public OGSScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void INITIALIZE(Context context) {
        int threadId = context.thisThreadId;
//        tpg.constructTPG(context);
        tpg.firstTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
    }

    protected void ProcessedToNextLevel(Context context) {
        context.currentLevel += 1;
        assert context.currentLevel <= context.maxLevel;
        context.currentLevelIndex = 0;
    }

//    @Override
//    public void PROCESS(Context context, long mark_ID) {
//        int threadId = context.thisThreadId;
//        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
//        SchedulingUnit next = next(context);
//        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
//        if (next != null) {
////            execute(context, next.getOperations(), mark_ID);
//            if (executeWithBusyWait(context, next, mark_ID)) {
//                MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
//                NOTIFY(next, context);
//                MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
//            }
//        } else {
//            next = nextFromBusyWaitQueue(context);
//            if (next != null) {
//                if(executeWithBusyWait(context, next, mark_ID)) {
//                    MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
//                    NOTIFY(next, context);
//                    MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
//                }
//            }
//        }
//    }

    /**
     * Used by OGBFSScheduler.
     *
     * @param context
     * @param operation_chain
     * @param mark_ID
     */
    public void execute(Context context, MyList<ExecutionUnit> operation_chain, long mark_ID) {
        for (ExecutionUnit operation : operation_chain) {
            execute(operation, mark_ID, false);
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    @Override
    protected SchedulingUnit next(Context context) {
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
