package common.bolts.transactional.lb;

import combo.SINKCombo;
import common.param.lb.LBEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;
import transaction.context.TxnContext;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class LBBolt extends TransactionalBolt {
    public SINKCombo sink;

    public LBBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "gs";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

//    protected void IBWJ_LOCK_AHEAD(LBEvent event, TxnContext txnContext) throws DatabaseException {
//        transactionManager.lock_ahead(txnContext, "index_r_table", event.getKey(), event.srcIndexRecordRef, READ_WRITE);
//        transactionManager.lock_ahead(txnContext, "index_s_table", event.getKey(), event.tarIndexRecordRef, READ_WRITE);
//    }
//
//    protected void IBWJ_REQUEST_NOLOCK(LBEvent event, TxnContext txnContext) throws DatabaseException {
//        transactionManager.SelectKeyRecord_noLock(txnContext, "index_r_table", event.getKey(), event.srcIndexRecordRef, READ_WRITE);
//        transactionManager.SelectKeyRecord_noLock(txnContext, "index_s_table", event.getKey(), event.tarIndexRecordRef, READ_WRITE);
//        assert event.srcIndexRecordRef.getRecord() != null || event.tarIndexRecordRef.getRecord() != null;
//    }
//
//    protected void IBWJ_REQUEST_CORE(LBEvent event) {
//        DataBox sourceIndex_addr = event.srcIndexRecordRef.getRecord().getValues().get(1);
//        DataBox sourceIndex_matching_addr = event.srcIndexRecordRef.getRecord().getValues().get(2);
//        DataBox targetIndex_addr = event.tarIndexRecordRef.getRecord().getValues().get(1);
//        final String targetIndexAddress = targetIndex_addr.getString();
//
//        sourceIndex_addr.setString(event.getAddress());
//        sourceIndex_matching_addr.setString(targetIndexAddress);
//    }

    protected void LAL_PROCESS(double _bid) throws InterruptedException, DatabaseException {
    }

    protected void POST_PROCESS(double _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            ((LBEvent) input_event).setTimestamp(timestamp);
            LB_POST((LBEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected boolean LB_CORE(LBEvent event) { //TODO: tstream
        SchemaRecordRef ref = event.serverRecord;
        if (ref.isEmpty()) {
            return false;//not yet processed.
        }
        DataBox dataBox = ref.getRecord().getValues().get(1); //Read address of matching tuple TODO: Verify this
        event.serverID = dataBox.getString();
        return true;
    }

    protected void LB_POST(LBEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }
}
