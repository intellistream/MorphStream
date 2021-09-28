package scheduler.impl.nonlayered;

import profiler.MeasureTools;
import scheduler.context.AbstractGSTPGContext;
import scheduler.context.GSTPGContextWithAbort;
import scheduler.impl.Scheduler;
import scheduler.struct.gs.AbstractGSOperationChain;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChainWithAbort;
import scheduler.struct.gs.GSOperationWithAbort;
import transaction.impl.ordered.MyList;

public abstract class AbstractGSScheduler<Context extends AbstractGSTPGContext<ExecutionUnit, SchedulingUnit>, ExecutionUnit extends GSOperation, SchedulingUnit extends AbstractGSOperationChain<ExecutionUnit>>
        extends Scheduler<Context, ExecutionUnit, SchedulingUnit> {

    public AbstractGSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(Context context) {
        tpg.firstTimeExploreTPG(context);
    }

    /**
     * // O1 -> (logical)  O2
     * // T1: pickup O1. Transition O1 (ready - > execute) || notify O2 (speculative -> ready).
     * // T2: pickup O2 (speculative -> executed)
     * // T3: pickup O2
     * fast explore dependencies in TPG and put ready/speculative operations into task queues.
     *
     * @param context
     */
    @Override
    public void EXPLORE(Context context) {
        throw new UnsupportedOperationException("Unsupported.");
    }

    @Override
    public boolean FINISHED(Context context) {
        return context.finished();
    }

    @Override
    public void TxnSubmitFinished(Context context) {
        throw new UnsupportedOperationException("Unsupported.");
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);
        SchedulingUnit next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        if (next != null) {
//            assert !next.getOperations().isEmpty();
            execute(context, next, mark_ID); // only when executed, the notification will start.
            MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
            if (next.hasChildren()) {
                NOTIFY(next, context);
            } else {
                next.isExecuted = true;
                next.context.scheduledOPs += next.getOperations().size();
            }
            MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
        } else {
            next = nextFromBusyWaitQueue(context);
            if (next != null) {
//                assert !next.getOperations().isEmpty();
                if (executeWithBusyWait(context, next, mark_ID)) { // only when executed, the notification will start.
                    next.isExecuted = true;
                    next.context.scheduledOPs += next.getOperations().size();
                }
            }
        }
     }

    @Override
    protected void NOTIFY(SchedulingUnit task, Context context) {
        throw new UnsupportedOperationException("Unsupported.");
    }

    /**
     * Used by GSScheduler.
     *  @param context
     * @param operationChain
     * @param mark_ID
     * @return
     */
    public boolean execute(Context context, SchedulingUnit operationChain, long mark_ID) {
        MyList<ExecutionUnit> operation_chain_list = operationChain.getOperations();
        for (ExecutionUnit operation : operation_chain_list) {
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
        }
        return true;
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    protected SchedulingUnit next(Context context) {
        SchedulingUnit operationChain = context.OCwithChildren.pollLast();
        if (operationChain == null) {
            operationChain = context.IsolatedOC.pollLast();
        }
        return operationChain;
    }

    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     */
    @Override
    public void DISTRIBUTE(SchedulingUnit task, Context context) {
        if (task != null) {
            if (!task.hasChildren()) {
                context.IsolatedOC.add(task);
            } else {
                context.OCwithChildren.add(task);
            }
        }
    }

}
