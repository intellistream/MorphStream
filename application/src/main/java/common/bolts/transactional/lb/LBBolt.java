package common.bolts.transactional.lb;

import combo.SINKCombo;
import common.param.lb.LBEvent;
import components.operators.api.TransactionalBolt;
import content.common.CommonMetaTypes;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import utils.AppConfig;

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

    protected void LB_NOCC_REQUEST(LBEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i=0; i<event.TOTAL_NUM_ACCESS; ++i) {
            boolean rt = transactionManager.SelectKeyRecord(txnContext, "server_table",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_WRITE); //TODO: _noLock? or _?
            assert !rt || event.getRecord_refs()[i].getRecord() != null;
        }
    }

    protected void LB_LOCK_AHEAD(LBEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i)
            transactionManager.lock_ahead(txnContext, "server_table",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_WRITE);
    }

    protected boolean lb_request_noLock(LBEvent event, TxnContext txnContext) throws DatabaseException {
        return !process_request_noLock(event, txnContext, READ_WRITE);
    }

    private boolean process_request_noLock(LBEvent event, TxnContext txnContext, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "server_table",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    protected void LB_CORE(LBEvent event) { //Used in LB_nocc & SStore
//        long start = System.nanoTime();
        DataBox TargetValue_value = event.getRecord_refs()[0].getRecord().getValues().get(1);
        int NUM_ACCESS = event.TOTAL_NUM_ACCESS;
        AppConfig.randomDelay();
        for (int i = 0; i < NUM_ACCESS; ++i) {
            SchemaRecordRef recordRef = event.getRecord_refs()[i];
            SchemaRecord record = recordRef.getRecord();
            DataBox Value_value = record.getValues().get(1);
        }
        TargetValue_value.incLong(1);
        event.serverID = event.getRecord_refs()[0].getRecord().getValues().get(0).getString();
    }

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

    protected boolean LB_TS_CORE(LBEvent event) { // tstream
        SchemaRecordRef ref = event.getRecord_refs()[0];
        if (ref.isEmpty()) {
            return false;//not yet processed.
        }
        DataBox dataBox = ref.getRecord().getValues().get(0); //Read server ID
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
