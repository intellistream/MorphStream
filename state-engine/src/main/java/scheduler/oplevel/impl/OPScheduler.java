package scheduler.oplevel.impl;


import content.T_StreamContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.impl.IScheduler;
import scheduler.oplevel.context.OPSchedulerContext;
import scheduler.oplevel.struct.AbstractOperation;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import transaction.function.DEC;
import transaction.function.INC;

import java.util.List;
import java.util.Map;

public abstract class OPScheduler<Context extends OPSchedulerContext, Task> implements IScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPScheduler.class);

    // DD: Transfer event processing
    protected void CT_Transfer_Fun(AbstractOperation operation, long previous_mark_ID, boolean clean) {
        SchemaRecord preValues = operation.condition_records[0].content_.readPreValues(operation.bid);
        SchemaRecord preValues1 = operation.condition_records[1].content_.readPreValues(operation.bid);
        if (preValues == null) {
            log.info("Failed to read condition records[0]" + operation.condition_records[0].record_.GetPrimaryKey());
            log.info("Its version size:" + ((T_StreamContent) operation.condition_records[0].content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[0].content_).versions.entrySet()) {
                log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
            }
            log.info("TRY reading:" + operation.condition_records[0].content_.readPreValues(operation.bid));//not modified in last round);
        }
        if (preValues1 == null) {
            log.info("Failed to read condition records[1]" + operation.condition_records[1].record_.GetPrimaryKey());
            log.info("Its version size:" + ((T_StreamContent) operation.condition_records[1].content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[1].content_).versions.entrySet()) {
                log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
            }
            log.info("TRY reading:" + ((T_StreamContent) operation.condition_records[1].content_).versions.get(operation.bid));//not modified in last round);
        }
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
            log.info("++++++ operation failed: "
                    + sourceAccountBalance + "-" + operation.condition.arg1
                    + " : " + sourceAccountBalance + "-" + operation.condition.arg2
                    + " : " + sourceAssetValue + "-" + operation.condition.arg3
                    + " condition: " + operation.condition);
        }
    }

    protected void CT_Depo_Fun(AbstractOperation operation, long mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(1).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }

    protected abstract void DISTRIBUTE(Task task, Context context);

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
}
