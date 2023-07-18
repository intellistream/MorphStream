package intellistream.morphstream.engine.txn.scheduler.impl.op.nonstructured;

import intellistream.morphstream.engine.txn.durability.struct.FaultToleranceRelax;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPNSAContext;
import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_path;

// TODO: code clean, a lot...
public class OPNSAScheduler<Context extends OPNSAContext> extends OPNSScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPNSAScheduler.class);

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public OPNSAScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void INITIALIZE(Context context) {
        tpg.firstTimeExploreTPG(context);
        if (tpg.isLogging == LOGOption_path && FaultToleranceRelax.isSelectiveLogging) {
            this.loggingManager.selectiveLoggingPartition(context.thisThreadId);
        }
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
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
        if (isLogging == LOGOption_path) {
            context.partitionStateManager.handleStateTransitionsWithAbortTracking(this.tpg.threadToPathRecord.get(context.thisThreadId));
        } else {
            context.partitionStateManager.handleStateTransitions();
        }
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
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);

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
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            execute(operation, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        }


        while (context.batchedOperations.size() != 0) {
            Operation remove = context.batchedOperations.remove();
            MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
            NOTIFY(remove, context); // this also covers abort handling logic
            MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
        }
//        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
    }

    @Override
    protected void NOTIFY(Operation operation, Context context) {
        operation.context.getListener().onOpProcessed(operation);
    }


    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    public Operation next(Context context) {
        return context.taskQueues.pollLast();
    }


    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     *
     * @param executableOperation
     * @param context
     */
    @Override
    public void DISTRIBUTE(Operation executableOperation, Context context) {
        if (executableOperation != null)
            context.taskQueues.add(executableOperation);
    }

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