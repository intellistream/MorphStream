package scheduler.oplevel.impl.tpg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.oplevel.context.OPGSTPGContext;
import scheduler.oplevel.impl.OPScheduler;
import scheduler.oplevel.struct.MetaTypes.OperationStateType;
import scheduler.oplevel.struct.Operation;
import utils.SOURCE_CONTROL;

import static common.CONTROL.enable_log;

public class OPGSScheduler<Context extends OPGSTPGContext> extends OPScheduler<Context, Operation> {
    private static final Logger log = LoggerFactory.getLogger(OPGSScheduler.class);

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public boolean needAbortHandling = false;

    public OPGSScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void start_evaluation(Context context, long mark_ID, int num_events) {
        int threadId = context.thisThreadId;
//        System.out.println(threadId + " first explore tpg");

        INITIALIZE(context);
//        System.out.println(threadId + " first explore tpg complete, start to process");

        do {
            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            EXPLORE(context);
            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            PROCESS(context, mark_ID);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        } while (!FINISHED(context));
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        if (needAbortHandling) {
            if (enable_log) {
                log.info("need abort handling, rollback and redo");
            }
            // identify all aborted operations and transit the state to aborted.
            REINITIALIZE(context);
            // rollback to the starting point and redo.
            do {
                MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
                EXPLORE(context);
                MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
                MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
                PROCESS(context, mark_ID);
                MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            } while (!FINISHED(context));
        }
        RESET(context);//
        MeasureTools.SCHEDULE_TIME_RECORD(threadId, num_events);
    }


    @Override
    public void INITIALIZE(Context context) {
        tpg.firstTimeExploreTPG(context);
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
    }

    public void REINITIALIZE(Context context) {
        tpg.secondTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
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
        context.partitionStateManager.handleStateTransitions();
    }

    @Override
    public boolean FINISHED(Context context) {
        return context.finished();
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        context.requests.clear();
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int cnt = 0;
        int batch_size = 100;//TODO;
        int threadId = context.thisThreadId;

        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        do {
            Operation next = next(context);
            if (next == null) {
                break;
            }
            context.batchedOperations.push(next);
            cnt++;
            if (cnt > batch_size) {
                break;
            }
        } while (true);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        for (Operation operation : context.batchedOperations) {
            execute(operation, mark_ID, false);
        }

        while (!context.batchedOperations.isEmpty()) {
            Operation remove = context.batchedOperations.remove();
            MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
            if (remove.isFailed && !remove.getOperationState().equals(OperationStateType.ABORTED)) {
                needAbortHandling = true;
            }
            NOTIFY(remove, context);
            MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
        }
    }

    @Override
    protected void NOTIFY(Operation operation, Context context) {
        operation.context.getListener().onOpProcessed(operation);
    }


//    /**
//     * Try to get task from local queue.
//     *
//     * @param context
//     * @return
//     */
    public Operation next(Context context) {
        return context.taskQueues.pollLast();
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
//    protected Operation next(Context context) {
//        Operation operation = context.OCwithChildren.pollLast();
//        if (operation == null) {
//            operation = context.IsolatedOC.pollLast();
//        }
//        return operation;
//    }


    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     *
     * @param executableOperation
     * @param context
     */
    public void DISTRIBUTE(Operation executableOperation, Context context) {
        if (executableOperation != null)
            context.taskQueues.add(executableOperation);
    }
//    @Override
//    public void DISTRIBUTE(Operation executableOperation, Context context) {
//        if (task != null) {
//            if (!task.hasChildren()) {
//                context.IsolatedOC.add(task);
//            } else {
//                context.OCwithChildren.add(task);
//            }
//        }
//    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onExecutable(Operation operation) {
            DISTRIBUTE(operation, (Context) operation.context);//TODO: make it clear..
        }

        public void onOPFinalized(Operation operation) {
//            operation.context.operations.remove(operation);
            operation.context.scheduledOPs++;
        }

        public void onOPRollbacked(Operation operation) {
            operation.context.scheduledOPs--;
        }
    }
}