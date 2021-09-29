package scheduler.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.GSTPGContextWithAbort;
import scheduler.context.SchedulerContext;
import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;
import scheduler.struct.TaskPrecedenceGraph;
import scheduler.struct.gs.GSOperationChainWithAbort;
import scheduler.struct.gs.GSOperationWithAbort;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import transaction.function.DEC;
import transaction.function.INC;
import utils.SOURCE_CONTROL;

import java.util.List;

import static common.CONTROL.enable_log;
import static content.common.CommonMetaTypes.AccessType.*;

public abstract class Scheduler<Context extends SchedulerContext<SchedulingUnit>, ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>> implements IScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);
    public final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph<Context, SchedulingUnit, ExecutionUnit> tpg; // TPG to be maintained in this global instance.

    protected Scheduler(int totalThreads, int NUM_ITEMS) {
        delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads); // Check id generation in DateGenerator.
        this.tpg = new TaskPrecedenceGraph<>(totalThreads, delta);
    }

    /**
     * state to thread mapping
     *
     * @param key
     * @param delta
     * @return
     */
    public static int getTaskId(String key, Integer delta) {
        Integer _key = Integer.valueOf(key);
        return _key / delta;
    }

    public void start_evaluation(Context context, long mark_ID, int num_events) {
        int threadId = context.thisThreadId;
//        MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
        System.out.println(threadId + " first explore tpg");

        INITIALIZE(context);
        System.out.println(threadId + " first explore tpg complete, start to process");

//        MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);

        do {
            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            EXPLORE(context);
            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            PROCESS(context, mark_ID);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        } while (!FINISHED(context));
        RESET(context);//
        MeasureTools.SCHEDULE_TIME_RECORD(threadId, num_events);
    }

    /**
     * Transfer event processing
     *
     * @param operation
     * @param previous_mark_ID
     * @param clean
     */
    protected void Transfer_Fun(ExecutionUnit operation, long previous_mark_ID, boolean clean) {

        SchemaRecord preValues = operation.condition_records[0].content_.readPreValues(operation.bid);
        SchemaRecord preValues1 = operation.condition_records[1].content_.readPreValues(operation.bid);

//        SchemaRecord preValues = operation.fdParentOps[0] == null ?
//                operation.condition_records[0].content_.readPreValues(operation.bid) :
//                operation.condition_records[0].content_.readPreValues(operation.bid, operation.fdParentOps[0].bid);
//        assert preValues != null;
//        SchemaRecord preValues1 = operation.fdParentOps[1] == null ?
//                operation.condition_records[1].content_.readPreValues(operation.bid) :
//                operation.condition_records[1].content_.readPreValues(operation.bid, operation.fdParentOps[1].bid);
//        assert preValues1 != null;

//        if (preValues == null) {
//            if (enable_log)
//                log.info("Failed to read condition records[0]" + operation.condition_records[0].record_.GetPrimaryKey());
//            if (enable_log)
//                log.info("Its version size:" + ((T_StreamContent) operation.condition_records[0].content_).versions.size());
//            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[0].content_).versions.entrySet()) {
//                if (enable_log)
//                    log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
//            }
//            if (enable_log)
//                log.info("TRY reading:" + operation.condition_records[0].content_.readPreValues(operation.bid));//not modified in last round);
//        }
//        if (preValues1 == null) {
//            if (enable_log)
//                log.info("Failed to read condition records[1]" + operation.condition_records[1].record_.GetPrimaryKey());
//            if (enable_log)
//                log.info("Its version size:" + ((T_StreamContent) operation.condition_records[1].content_).versions.size());
//            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[1].content_).versions.entrySet()) {
//                if (enable_log)
//                    log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
//            }
//            if (enable_log)
//                log.info("TRY reading:" + ((T_StreamContent) operation.condition_records[1].content_).versions.get(operation.bid));//not modified in last round);
//        }
        final long sourceAccountBalance = preValues.getValues().get(1).getLong();
        final long sourceAssetValue = preValues1.getValues().get(1).getLong();

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
            if (enable_log) log.info("++++++ operation failed: "
                    + sourceAccountBalance + "-" + operation.condition.arg1
                    + " : " + sourceAccountBalance + "-" + operation.condition.arg2
                    + " : " + sourceAssetValue + "-" + operation.condition.arg3
                    + " condition: " + operation.condition);
        }
    }

    /**
     * Deposite event processing
     *
     * @param operation
     * @param mark_ID
     * @param clean
     */
    protected void Depo_Fun(ExecutionUnit operation, long mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(1).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }

    /**
     * general operation execution entry method for all schedulers.
     *
     * @param operation
     * @param mark_ID
     * @param clean
     */
    public void execute(ExecutionUnit operation, long mark_ID, boolean clean) {
        if (operation.aborted) {
            return; // return if the operation is already aborted
        }
        int success;
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            success = operation.success[0];
            Transfer_Fun(operation, mark_ID, clean);
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
            if (operation.success[0] == success) {
                operation.isFailed = true;
            } else {
                operation.isExecuted = true;
            }
        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            success = operation.success[0];
            Transfer_Fun(operation, mark_ID, clean);
            if (operation.success[0] == success) {
                operation.isFailed = true;
            } else {
                operation.isExecuted = true;
            }
        } else if (operation.accessType.equals(READ_WRITE)) {
            Depo_Fun(operation, mark_ID, clean);
            operation.isExecuted = true;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public boolean executeWithBusyWait(Context context, SchedulingUnit operationChain, long mark_ID) {
        throw new UnsupportedOperationException();
    }

    protected SchedulingUnit nextFromBusyWaitQueue(Context context) {
        return context.busyWaitQueue.poll();
    }

    protected abstract void DISTRIBUTE(SchedulingUnit task, Context context);

    protected abstract void NOTIFY(SchedulingUnit task, Context context);

    @Override
    public boolean FINISHED(Context context) {
        return context.finished();
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
        context.push(request);
        return false;
    }

    @Override
    public void RESET(Context context) {
        SOURCE_CONTROL.getInstance().oneThreadCompleted();
        context.reset();
        tpg.reset(context);
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        context.requests.clear();
    }

    @Override
    public abstract void TxnSubmitFinished(Context context);

    @Override
    public void AddContext(int threadId, Context context) {
        tpg.threadToContextMap.put(threadId, context);
    }

    public Context getTargetContext(TableRecord d_record) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId = getTaskId(d_record.record_.GetPrimaryKey(), delta);
        return tpg.threadToContextMap.get(threadId);
    }

    protected boolean isConflicted(Context context, SchedulingUnit operationChain, ExecutionUnit operation) {
        if (operation.fdParentOps != null) {
            if (operation.fdParentOps[0] != null) {
                if (!operation.fdParentOps[0].isExecuted) {
                    // blocked and busy wait
                    context.busyWaitQueue.add(operationChain);
                    return true;
                }
            }
            if (operation.fdParentOps[1] != null) {
                if (!operation.fdParentOps[1].isExecuted) {
                    context.busyWaitQueue.add(operationChain);
                    return true;
                }
            }
        }
        return false;
    }
}
