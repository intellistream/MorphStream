package scheduler.oplevel.impl.tpg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.oplevel.context.OPGSTPGContextWithAbort;
import scheduler.oplevel.struct.MetaTypes.OperationStateType;
import scheduler.oplevel.struct.Operation;
import utils.SOURCE_CONTROL;

import static content.common.CommonMetaTypes.AccessType.*;

// TODO: code clean, a lot...
public class OPGSSchedulerWithAbort<Context extends OPGSTPGContextWithAbort> extends OPGSScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPGSSchedulerWithAbort.class);

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public OPGSSchedulerWithAbort(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(Context context) {
        tpg.firstTimeExploreTPG(context);
        context.partitionStateManager.initialize(executableTaskListener);
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
    public void TxnSubmitFinished(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        int txnOpId = 0;
        Operation headerOperation = null;
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            Operation set_op;
            switch (request.accessType) {
                case READ_WRITE: // they can use the same method for processing
                case READ_WRITE_COND:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            tpg.setupOperationTDFD(set_op, request);
            if (txnOpId == 0)
                headerOperation = set_op;
            // addOperation an operation id for the operation for the purpose of temporal dependency construction
            set_op.setTxnOpId(txnOpId++);
            set_op.addHeader(headerOperation);
            headerOperation.addDescendant(set_op);
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
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