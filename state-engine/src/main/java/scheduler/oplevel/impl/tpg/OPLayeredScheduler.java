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

    public OPLayeredScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
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
                    set_op = new Operation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                    set_op = new Operation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
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

    /**
     * Used by tpgScheduler.
     *
     * @param operation
     * @param mark_ID
     * @param clean
     */
    public void execute(Operation operation, long mark_ID, boolean clean) {
        log.trace("++++++execute: " + operation);
        // if the operation is in state aborted or committable or committed, we can bypass the execution
        if (operation.getOperationState().equals(OperationStateType.ABORTED)
                || operation.getOperationState().equals(OperationStateType.COMMITTABLE)
                || operation.getOperationState().equals(OperationStateType.COMMITTED)) {
            log.trace("++++++bypassed: " + operation);
            //otherwise, skip (those already been tagged as aborted).
            return;
        }
        int success;
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            success = operation.success[0];
            CT_Transfer_Fun(operation, mark_ID, clean);
            // check whether needs to return a read results of the operation
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
            // operation success check, number of operation succeeded does not increase after execution
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            success = operation.success[0];
            CT_Transfer_Fun(operation, mark_ID, clean);
            // operation success check, number of operation succeeded does not increase after execution
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else if (operation.accessType.equals(READ_WRITE)) {
            CT_Depo_Fun(operation, mark_ID, clean);
        } else {
            throw new UnsupportedOperationException();
        }

        assert operation.getOperationState() != OperationStateType.EXECUTED;
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

        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        for (Operation operation : context.batchedOperations) {
            execute(operation, mark_ID, false);
        }

        while (context.batchedOperations.size() != 0) {
            Operation remove = context.batchedOperations.remove();
            NOTIFY(remove, context);
        }
        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
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