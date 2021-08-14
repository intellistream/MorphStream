package transaction.scheduler;


import content.T_StreamContent;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import transaction.function.DEC;
import transaction.function.INC;
import transaction.scheduler.common.AbstractOperation;

import java.util.List;
import java.util.Map;

@lombok.extern.slf4j.Slf4j
public abstract class Scheduler<Context, Task> implements IScheduler<Context> {

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
        synchronized (operation.success) {
            operation.success[0]++;
        }
    }

    protected abstract void DISTRIBUTE(Task task, Context context);
}
