package scheduler.impl.og.nonstructured;

import scheduler.context.og.AbstractOGNSContext;
import scheduler.impl.og.OGScheduler;
import scheduler.struct.og.nonstructured.AbstractNSOperationChain;
import scheduler.struct.og.nonstructured.NSOperation;
import transaction.impl.ordered.MyList;

public abstract class AbstractOGNSScheduler<Context extends AbstractOGNSContext<ExecutionUnit, SchedulingUnit>, ExecutionUnit extends NSOperation, SchedulingUnit extends AbstractNSOperationChain<ExecutionUnit>>
        extends OGScheduler<Context, ExecutionUnit, SchedulingUnit> {

    public AbstractOGNSScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
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
    protected void NOTIFY(SchedulingUnit task, Context context) {
        throw new UnsupportedOperationException("Unsupported.");
    }

    /**
     * Used by OGNSScheduler.
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
    @Override
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
