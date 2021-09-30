package scheduler.oplevel.impl.tpg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.oplevel.context.OPGSTPGContext;
import scheduler.oplevel.impl.OPScheduler;
import scheduler.oplevel.struct.MetaTypes.OperationStateType;
import scheduler.oplevel.struct.Operation;
import scheduler.oplevel.struct.TaskPrecedenceGraph;
import storage.TableRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static content.common.CommonMetaTypes.AccessType.*;

public class OPGSScheduler<Context extends OPGSTPGContext> extends OPScheduler<Context, Operation> {
    private static final Logger log = LoggerFactory.getLogger(OPGSScheduler.class);

    protected final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph tpg; // TPG to be maintained in this global instance.
    public final Map<Integer, Context> threadToContextMap;

    public OPGSScheduler(int tp, int NUM_ITEMS) {
        delta = (int) Math.ceil(NUM_ITEMS / (double) tp); // Check id generation in DateGenerator.
        this.tpg = new TaskPrecedenceGraph(tp);
        ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();
        tpg.setExecutableListener(executableTaskListener);
        threadToContextMap = new HashMap<>();
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
        while (context.batchedOperations.size() != 0) {
            Operation remove = context.batchedOperations.remove();
            remove.context.partitionStateManager.onOpProcessed(remove);
        }
        context.partitionStateManager.handleStateTransitions();
    }

    @Override
    public boolean FINISHED(Context threadId) {
        return tpg.isFinished();
    }

    @Override
    public void RESET(Context context) {

    }

    /**
     * Submit requests to target thread --> data shuffling is involved.
     *
     * @param context
     * @param request
     * @return
     */
    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.requests.push(request);
        return false;
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        context.requests.clear();
    }

    @Override
    public void AddContext(int threadId, Context context) {
        threadToContextMap.put(threadId, context);
    }

    @Override
    public void TxnSubmitFinished(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<Operation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            Operation set_op = null;
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
            }
            operationGraph.add(set_op);
            tpg.setupOperationTDFD(set_op, request);
        }

        // 4. send operation graph to tpg for tpg construction
        tpg.setupOperationLD(operationGraph);//TODO: this is bad refactor.
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    private Context getTargetContext(TableRecord d_record) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId = getTaskId(d_record.record_.GetPrimaryKey());
        return threadToContextMap.get(threadId);
    }

    private int getTaskId(String key) {
        int _key = Integer.parseInt(key);
        return _key / delta;
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
        int success = operation.success[0];
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            CT_Transfer_Fun(operation, mark_ID, clean);
            // check whether needs to return a read results of the operation
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            CT_Transfer_Fun(operation, mark_ID, clean);
        } else if (operation.accessType.equals(READ_WRITE)) {
            CT_Depo_Fun(operation, mark_ID, clean);
        } else {
            throw new UnsupportedOperationException();
        }
        // operation success check, number of operation succeeded does not increase after execution
        if (operation.success[0] == success) {
            operation.isFailed = true;
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
        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
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
    protected void DISTRIBUTE(Operation executableOperation, Context context) {
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
    }
}