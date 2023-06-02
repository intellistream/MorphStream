package common.bolts.transactional.ibwj;

import combo.SINKCombo;
import common.param.ibwj.IBWJEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
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

import java.util.List;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.*;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class IBWJBolt extends TransactionalBolt {
    public SINKCombo sink;

    public IBWJBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "gs";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void IBWJ_LOCK_AHEAD(IBWJEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.lock_ahead(txnContext, "index_r_table", event.getKey(), event.srcIndexRecordRef, READ_WRITE);
        transactionManager.lock_ahead(txnContext, "index_s_table", event.getKey(), event.tarIndexRecordRef, READ_WRITE);
    }

    protected void IBWJ_REQUEST_NOLOCK(IBWJEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.SelectKeyRecord_noLock(txnContext, "index_r_table", event.getKey(), event.srcIndexRecordRef, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "index_s_table", event.getKey(), event.tarIndexRecordRef, READ_WRITE);
        assert event.srcIndexRecordRef.getRecord() != null || event.tarIndexRecordRef.getRecord() != null;
    }

    protected void IBWJ_REQUEST_CORE(IBWJEvent event) {
        DataBox sourceIndex_addr = event.srcIndexRecordRef.getRecord().getValues().get(1);
        DataBox sourceIndex_matching_addr = event.srcIndexRecordRef.getRecord().getValues().get(2);
        DataBox targetIndex_addr = event.tarIndexRecordRef.getRecord().getValues().get(1);
        final String targetIndexAddress = targetIndex_addr.getString();

        sourceIndex_addr.setString(event.getAddress());
        sourceIndex_matching_addr.setString(targetIndexAddress);
    }

    protected void LAL_PROCESS(double _bid) throws InterruptedException, DatabaseException {
    }

    protected void POST_PROCESS(double _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            ((IBWJEvent) input_event).setTimestamp(timestamp);
            IBWJ_POST((IBWJEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected boolean IBWJ_CORE(IBWJEvent event) { //TODO: tstream
        SchemaRecordRef ref = event.srcIndexRecordRef;
        if (ref.isEmpty()) {
            return false;//not yet processed.
        }
        DataBox dataBox = ref.getRecord().getValues().get(1); //Read address of matching tuple TODO: Verify this
        event.setAddressTuple(dataBox.getString());
        return true;
    }

    protected void IBWJ_POST(IBWJEvent event) throws InterruptedException { //TODO: tstream
        String joinResult = event.getAddressTuple().toString();
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, joinResult, event.getTimestamp())));
            }
        }
    }
}
