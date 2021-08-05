package transaction.scheduler.tpg;

import content.T_StreamContent;
import index.high_scale_lib.ConcurrentHashMap;
import profiler.MeasureTools;
import storage.SchemaRecord;
import transaction.TxnProcessingEngine;
import transaction.function.DEC;
import transaction.function.INC;
import transaction.scheduler.Scheduler;
import transaction.scheduler.tpg.struct.Controller;
import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import static common.meta.CommonMetaTypes.AccessType.GET;
import static common.meta.CommonMetaTypes.AccessType.SET;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 */
@lombok.extern.slf4j.Slf4j
public class TPGScheduler extends Scheduler<Operation> {
    private final int totalThreads;
    public final TaskPrecedenceGraph tpg; // TPG to be maintained in this global instance.
    private final ConcurrentHashMap<Integer, ConcurrentLinkedDeque<Operation>> taskQueues; // task queues to store operations for each thread

    public TPGScheduler(int tp) {
        totalThreads = tp;
        this.tpg = new TaskPrecedenceGraph(totalThreads);
        taskQueues = new ConcurrentHashMap<>();
        for (int i = 0; i < totalThreads; i++) {
            taskQueues.put(i, new ConcurrentLinkedDeque<>());
        }
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
        DISTRIBUTE(tpg.exploreReady(), threadId);
        DISTRIBUTE(tpg.exploreSpeculative(), threadId);
    }
    @Override
    public boolean FINISHED(int threadId) {
        return tpg.isFinished();
    }
    @Override
    public void RESET() {
        Controller.exec.shutdownNow();
    }

    // DD: Transfer event processing
    private void CT_Transfer_Fun(int threadId, transaction.scheduler.tpg.struct.Operation operation, long previous_mark_ID, boolean clean) {
        Queue<Operation> fd_parents = operation.getParents(MetaTypes.DependencyType.FD);
        List<SchemaRecord> preValues = new ArrayList<>();
        for (transaction.scheduler.tpg.struct.Operation parent : fd_parents) {
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
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record

            if (operation.function instanceof INC) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else if (operation.function instanceof DEC) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            operation.success[0] = true;
        } else {
            log.info("++++++ operation failed: "
                    + sourceAccountBalance + "-" + operation.condition.arg1
                    + " : " + sourceAccountBalance + "-" + operation.condition.arg2
                    + " : " + sourceAssetValue + "-" + operation.condition.arg3
                    + " condition: " + operation.condition);
            operation.success[0] = false;
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
    public void process(int threadId, transaction.scheduler.tpg.struct.Operation operation, long mark_ID, boolean clean)
    {
        log.trace("++++++execute: " + operation);
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.READY) || operation.getOperationState().equals(MetaTypes.OperationStateType.SPECULATIVE)) {
            // the operation will only be executed when the state is in READY/SPECULATIVE,
            boolean isFailed = false;
            if (operation.accessType.equals(GET)) {
                SchemaRecord schemaRecord = operation.d_record.content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType);
                // operation success check
                if (schemaRecord == null) {
                    isFailed = true;
                } else {
                    operation.record_ref.setRecord(new SchemaRecord(schemaRecord.getValues()));//Note that, locking scheme allows directly modifying on original table d_record.
                }
            } else if (operation.accessType.equals(SET)) {
                log.debug("++++++execute: " + operation);
                CT_Transfer_Fun(threadId, operation, mark_ID, clean);
                // operation success check
                if (!operation.success[0]) {
                    isFailed = true;
                } else {
                    if (operation.record_ref != null) {
                        operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
                    }
                }
            } else {
                throw new UnsupportedOperationException();
            }
            assert operation.getOperationState() != MetaTypes.OperationStateType.EXECUTED;
//            operation.onProcessed(isFailed);
            Controller.stateManagers.get(threadId).onProcessed(operation, isFailed);
        }//otherwise, skip (those already been tagged as aborted).
    }
    
    @Override
    public void PROCESS(int threadId, long mark_ID) {
        do {
            MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
            Operation next = next(threadId);
            MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
            if (next == null) break;
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            process(threadId, next, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        } while (true);
    }

    /**
     * Try to get task from local queue.
     *
     * @param threadId
     * @return
     */
    public Operation next(int threadId) {
        ConcurrentLinkedDeque<Operation> taskQueue = taskQueues.get(threadId);
        if (taskQueue.size() > 0)
            return taskQueue.removeLast();
        else return null;
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
}
