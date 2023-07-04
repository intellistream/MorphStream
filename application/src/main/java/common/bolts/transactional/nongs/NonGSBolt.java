package common.bolts.transactional.nongs;

import combo.SINKCombo;
import common.param.mb.NonMicroEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;

public abstract class NonGSBolt extends TransactionalBolt {
    public SINKCombo sink;
    public NonGSBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "non_gs";
    }
    protected boolean READ_CORE(NonMicroEvent event) {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];
            if (ref.isEmpty()) {
                return false;//not yet processed.
            }
            DataBox dataBox = ref.getRecord().getValues().get(1);
            int read_result = Integer.parseInt(dataBox.getString().trim());
            event.result[i] = read_result;
        }
        return true;
    }
    protected void READ_POST(NonMicroEvent event) throws InterruptedException {
        int sum = 0;
        if (POST_COMPUTE_COMPLEXITY != 0) {
            for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
                sum += event.result[i];
            }
            for (int j = 0; j < POST_COMPUTE_COMPLEXITY; ++j)
                sum += System.nanoTime();
        }
//        com_result = sum;
        if (enable_speculative) {
            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
            //now we assume it's all correct for testing its upper bond.
            //so nothing is send out.
        } else {
            if (!enable_app_combo) {
                collector.emit(event.getBid(), sum, event.getTimestamp());//the tuple is finished finally.
            } else {
                if (enable_latency_measurement) {
                    sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, sum, event.getTimestamp())));//(double bid, int sourceId, TopologyContext context, Message message)
                }
            }
        }
        sum = 0;
    }
    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }
}