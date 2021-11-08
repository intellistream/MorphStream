package scheduler.oplevel.impl.tpg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.oplevel.context.OPLayeredContext;
import scheduler.oplevel.impl.OPScheduler;
import scheduler.oplevel.struct.MetaTypes.OperationStateType;
import scheduler.oplevel.struct.Operation;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;

import static content.common.CommonMetaTypes.AccessType.*;

public class OPLayeredScheduler<Context extends OPLayeredContext> extends OPScheduler<Context, Operation> {
    private static final Logger log = LoggerFactory.getLogger(OPLayeredScheduler.class);

    public OPLayeredScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void INITIALIZE(Context context) {
        tpg.firstTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
    }

    protected void ProcessedToNextLevel(Context context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
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
        Operation next = Next(context);
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

//        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        for (Operation operation : context.batchedOperations) {
            execute(operation, mark_ID, false);
        }

        while (context.batchedOperations.size() != 0) {
            Operation remove = context.batchedOperations.remove();
            MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
            NOTIFY(remove, context);
            MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
        }
//        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
    }

    @Override
    protected void NOTIFY(Operation task, Context context) {
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    public Operation next(Context context) {
        Operation operation = context.ready_oc;
        context.ready_oc = null;
        return operation;
    }

    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     *
     * @param task
     * @param context
     */
    @Override
    protected void DISTRIBUTE(Operation task, Context context) {
        context.ready_oc = task;
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param context
     * @return
     */
    protected Operation Next(Context context) {
        ArrayList<Operation> ops = context.OPSCurrentLayer();
        Operation op = null;
        if (ops != null && context.currentLevelIndex < ops.size()) {
            op = ops.get(context.currentLevelIndex++);
            context.scheduledOPs++;
        }
        return op;
    }
}