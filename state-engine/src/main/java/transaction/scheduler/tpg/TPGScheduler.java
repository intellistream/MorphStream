package transaction.scheduler.tpg;

import common.meta.CommonMetaTypes;
import content.T_StreamContent;
import index.high_scale_lib.ConcurrentHashMap;
import profiler.MeasureTools;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.TableRecord;
import transaction.function.DEC;
import transaction.function.INC;
import transaction.impl.TxnContext;
import transaction.scheduler.Request;
import transaction.scheduler.Scheduler;
import transaction.scheduler.tpg.struct.Controller;
import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

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
public class TPGScheduler extends Scheduler<Operation> {
    public final TaskPrecedenceGraph tpg; // TPG to be maintained in this global instance.
//    private final Map<Integer, ConcurrentLinkedDeque<Operation>> taskQueues; // task queues to store operations for each thread
    private final Map<Integer, ConcurrentLinkedDeque<Operation>> taskQueues; // task queues to store operations for each thread
    private final Map<Integer, ArrayDeque<Request>> requests = new ConcurrentHashMap<>();
    public TPGScheduler(int tp) {
        this.tpg = new TaskPrecedenceGraph(tp);
        taskQueues = new HashMap<>();
        for (int i = 0; i < tp; i++) {
            taskQueues.put(i, new ConcurrentLinkedDeque<>());
            requests.put(i, new ArrayDeque<>());
        }
        ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();
        tpg.setExecutableListener(executableTaskListener);
    }

    @Override
    public void INITIALIZE(int threadId) {
        tpg.firstTimeExploreTPG(threadId);
        Controller.exec.submit(Controller.stateManagers.get(threadId));
    }
    /**
     * // O1 -> (logical)  O2
     * // T1: pickup O1. Transition O1 (ready - > execute) || notify O2 (speculative -> ready).
     * // T2: pickup O2 (speculative -> executed)
     * // T3: pickup O2
     * fast explore dependencies in TPG and put ready/speculative operations into task queues.
     *
     * @param threadId
     */
    @Override
    public void EXPLORE(int threadId) {
//        DISTRIBUTE(tpg.exploreReady(), threadId);
//        DISTRIBUTE(tpg.exploreSpeculative(), threadId);
    }
    @Override
    public boolean FINISHED(int threadId) {
        return tpg.isFinished();
    }
    @Override
    public void RESET() {
        Controller.exec.shutdownNow();
    }
    @Override
    public boolean SubmitRequest(Request request) {
        requests.get(request.txn_context.thread_Id).push(request);
        return false;
    }

    @Override
    public void TxnSubmitBegin(int thread_Id) {
        requests.get(thread_Id).clear();
    }
    private Operation[] constructGetOperation(TxnContext txn_context, String[] condition_sourceTable,
                                              String[] condition_source, TableRecord[] condition_records, long bid) {
        // read the s_record
        Operation[] get_ops = new Operation[condition_source.length];
        for (int index = 0; index < condition_source.length; index++) {
            TableRecord s_record = condition_records[index];
            String s_table_name = condition_sourceTable[index];
            SchemaRecordRef tmp_src_value = new SchemaRecordRef();
            get_ops[index] = new Operation(s_table_name, txn_context, bid, CommonMetaTypes.AccessType.GET, s_record, tmp_src_value);
        }
        return get_ops;
    }

    @Override
    public void TxnSubmitFinished(int thread_Id) {
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<Operation> operationGraph = new ArrayList<>();
        for (Request request : requests.get(thread_Id)) {
            long bid = request.txn_context.getBID();
            if (request.accessType.equals(CommonMetaTypes.AccessType.READ_WRITE_COND)) {
                // instead of use WRITE/READ/READ_WRITE primitives, we use GET/SET primitives with more flexibility.
                // 1. construct operations
                // read source record that to help the update on destination record
                Operation[] get_ops = constructGetOperation(request.txn_context, request.condition_sourceTable, request.condition_source, request.condition_records, bid);
                Operation set_op = new Operation(request.table_name, request.txn_context, bid, CommonMetaTypes.AccessType.SET, request.d_record, request.function, request.condition, request.success);
                // write the destination record, the write operation depends on the record returned by get_op

                // 2. add data dependencies, parent op will notify children op after it was executed
                for (Operation get_op : get_ops) {
                    get_op.addChild(set_op, MetaTypes.DependencyType.FD);
                    set_op.addParent(get_op, MetaTypes.DependencyType.FD);
                    operationGraph.add(get_op);
                }
                operationGraph.add(set_op);
            } else if (request.accessType.equals(CommonMetaTypes.AccessType.READ_WRITE_COND_READ)) {
                // instead of use WRITE/READ/READ_WRITE primitives, we use GET/SET primitives with more flexibility.
                // 1. construct operations
                // read the s_record
                Operation[] get_ops = constructGetOperation(request.txn_context, request.condition_sourceTable, request.condition_source, request.condition_records, bid);
                Operation set_op = new Operation(request.table_name, request.txn_context, bid, CommonMetaTypes.AccessType.SET, request.d_record, request.record_ref, request.function, request.condition, request.success);

                // 2. add data dependencies, parent op will notify children op after it was executed
                for (Operation get_op : get_ops) {
                    get_op.addChild(set_op, MetaTypes.DependencyType.FD);
                    set_op.addParent(get_op, MetaTypes.DependencyType.FD);
                    operationGraph.add(get_op);
                }
                operationGraph.add(set_op);
            }
        }

        // 4. send operation graph to tpg for tpg construction
        tpg.addTxn(operationGraph);//TODO: this is bad refactor.

    }

    // DD: Transfer event processing
    private void CT_Transfer_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        Queue<Operation> fd_parents = operation.getParents(MetaTypes.DependencyType.FD);
        List<SchemaRecord> preValues = new ArrayList<>();
        for (Operation parent : fd_parents) {
            preValues.add(parent.record_ref.getRecord());
        }

        if (preValues.get(0) == null) {
            log.info("Failed to read condition records[0]" + operation.condition_records[0].record_.GetPrimaryKey());
            log.info("Its version size:" + ((T_StreamContent) operation.condition_records[0].content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[0].content_).versions.entrySet()) {
                log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
            }
            log.info("TRY reading:" + operation.condition_records[0].content_.readPreValues(operation.bid));//not modified in last round);
        }
        if (preValues.get(1) == null) {
            log.info("Failed to read condition records[1]" + operation.condition_records[1].record_.GetPrimaryKey());
            log.info("Its version size:" + ((T_StreamContent) operation.condition_records[1].content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[1].content_).versions.entrySet()) {
                log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
            }
            log.info("TRY reading:" + ((T_StreamContent) operation.condition_records[1].content_).versions.get(operation.bid));//not modified in last round);
        }
        final long sourceAccountBalance = preValues.get(0).getValues().get(1).getLong();
        final long sourceAssetValue = preValues.get(1).getValues().get(1).getLong();
        if (sourceAccountBalance > operation.condition.arg1
                && sourceAccountBalance > operation.condition.arg2
                && sourceAssetValue > operation.condition.arg3) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            // apply function
            if (operation.function instanceof INC) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else if (operation.function instanceof DEC) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }
        } else {
            log.info("++++++ operation failed: "
                    + sourceAccountBalance + "-" + operation.condition.arg1
                    + " : " + sourceAccountBalance + "-" + operation.condition.arg2
                    + " : " + sourceAssetValue + "-" + operation.condition.arg3
                    + " condition: " + operation.condition);
        }
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
        boolean isFailed = false;
        if (operation.accessType.equals(GET)) {
            operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
        } else if (operation.accessType.equals(SET)) {
            int success = operation.success[0];
            CT_Transfer_Fun(operation, mark_ID, clean);
            // operation success check
            if (operation.success[0] == success) {
                isFailed = true;
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
        Controller.stateManagers.get(threadId).onProcessed(operation, isFailed);
    }

    @Override
    public void PROCESS(int threadId, long mark_ID) {
        do {
            MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
            Operation next = next(threadId);
            MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
            if (next == null) break;
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            execute(threadId, next, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        } while (true);
    }

//    public void measureTime(int threadId, Runnable runnable, Operation operation) {
////        long start = System.nanoTime();
//        runnable.run();
////        System.out.println(threadId + "|" + operation.accessType + "|[" + operation + "]|" + (System.nanoTime() - start));
//    }

    /**
     * Try to get task from local queue.
     *
     * @param threadId
     * @return
     */
    public Operation next(int threadId) {
        return taskQueues.get(threadId).pollLast();
//        if (taskQueue.size() > 0) {
//        System.out.println(threadId + " | " + Thread.currentThread().getId());
//            return taskQueue.removeLast();
//        }
//        return taskQueue.pollLast();
//        else return null;
    }
    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     */
    @Override
    protected void DISTRIBUTE(Operation executableOperation, int threadId) {
        if (executableOperation != null)
            taskQueues.get(threadId).add(executableOperation);
    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onExecutable(Operation operation, int threadId) {
            DISTRIBUTE(operation, threadId);
        }
    }
}
