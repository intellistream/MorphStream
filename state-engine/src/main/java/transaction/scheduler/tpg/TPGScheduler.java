package transaction.scheduler.tpg;

import common.meta.CommonMetaTypes;
import profiler.MeasureTools;
import storage.TableRecord;
import transaction.impl.TxnContext;
import transaction.scheduler.Request;
import transaction.scheduler.Scheduler;
import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationChain;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.*;

import static common.meta.CommonMetaTypes.AccessType.GET;
import static common.meta.CommonMetaTypes.AccessType.SET;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
@lombok.extern.slf4j.Slf4j
public class TPGScheduler<Context extends TPGContext> extends Scheduler<Context, OperationChain> {
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph tpg; // TPG to be maintained in this global instance.
    public final Map<Integer, Context> threadToContextMap;

    public TPGScheduler(int tp, int NUM_ITEMS) {
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
        context.partitionStateManager.handleStateTransitions();
    }

    @Override
    public boolean FINISHED(Context threadId) {
        return tpg.isFinished();
    }

    @Override
    public void RESET() {
//        Controller.exec.shutdownNow();
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

    private Operation[] constructGetOperation(TxnContext txn_context, String[] condition_sourceTable,
                                              String[] condition_source, TableRecord[] condition_records, long bid) {
        // read the s_record
        Operation[] get_ops = new Operation[condition_source.length];
        for (int index = 0; index < condition_source.length; index++) {
            TableRecord s_record = condition_records[index];
            String s_table_name = condition_sourceTable[index];
//            SchemaRecordRef tmp_src_value = new SchemaRecordRef();
            get_ops[index] = new Operation(getTargetContext(s_record), s_table_name, txn_context, bid, CommonMetaTypes.AccessType.GET, s_record);
        }
        return get_ops;
    }

    @Override
    public void TxnSubmitFinished(Context context) {
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<Operation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            Operation set_op = null;
            if (request.accessType.equals(CommonMetaTypes.AccessType.READ_WRITE_COND)) {
                set_op = new Operation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, CommonMetaTypes.AccessType.SET,
                        request.d_record, request.function, request.condition, request.condition_records, request.success);
            } else if (request.accessType.equals(CommonMetaTypes.AccessType.READ_WRITE_COND_READ)) {
                set_op = new Operation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, CommonMetaTypes.AccessType.SET,
                        request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
            }
            operationGraph.add(set_op);
            tpg.setupOperationChain(set_op, request);
        }

        // 4. send operation graph to tpg for tpg construction
        tpg.setupOperations(operationGraph);//TODO: this is bad refactor.
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
     * @param threadId
     * @param operation
     * @param mark_ID
     * @param clean
     */
    public void execute(int threadId, Operation operation, long mark_ID, boolean clean) {
        log.trace("++++++execute: " + operation);
        if (!(operation.getOperationState().equals(MetaTypes.OperationStateType.READY) || operation.getOperationState().equals(MetaTypes.OperationStateType.SPECULATIVE))) {
            //otherwise, skip (those already been tagged as aborted).
            return;
        }
        // the operation will only be executed when the state is in READY/SPECULATIVE,
        if (operation.accessType.equals(GET)) {
//            operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            // do nothing but notify set operation that it is executable
        } else if (operation.accessType.equals(SET)) {
            int success = operation.success[0];
            CT_Transfer_Fun(operation, mark_ID, clean);
            // operation success check
            if (operation.success[0] == success) {
                operation.isFailed = true;
            } else {
                // check whether needs to return a read results of the operation
                if (operation.record_ref != null) {
                    operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }
        assert operation.getOperationState() != MetaTypes.OperationStateType.EXECUTED;
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);
        OperationChain next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        if (next != null) {
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            for (Operation operation : next.getOperations()) {
                execute(threadId, operation, mark_ID, false);
            }
            next.context.partitionStateManager.onOcExecuted(next);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    public OperationChain next(Context context) {
        OperationChain operationChain = context.OCwithChildren.pollLast();
        if (operationChain == null) {
            return context.IsolatedOC.pollLast();
        }
        return operationChain;
    }


    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     *
     * @param executableOperationChain
     * @param context
     */
    @Override
    protected void DISTRIBUTE(OperationChain executableOperationChain, Context context) {
        if (executableOperationChain != null)
            if (!executableOperationChain.hasChildren())
                context.IsolatedOC.add(executableOperationChain);
            else
                context.OCwithChildren.add(executableOperationChain);
    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onExecutable(OperationChain operationChain) {
            DISTRIBUTE(operationChain, (Context) operationChain.context);//TODO: make it clear..
        }
    }
}
