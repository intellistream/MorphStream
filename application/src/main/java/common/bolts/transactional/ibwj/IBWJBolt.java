package common.bolts.transactional.ibwj;

import combo.SINKCombo;
import common.param.ed.tc.TCEvent;
import common.param.ibwj.IBWJEvent;
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

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.READ_ONLY;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;
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

    protected boolean IBWJ_CORE(IBWJEvent event) { //TODO: tstream
        SchemaRecordRef ref = event.getRecord_ref();
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


//    protected void READ_LOCK_AHEAD(IBWJEvent event, TxnContext txnContext) throws DatabaseException {
//        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i)
//            transactionManager.lock_ahead(txnContext, "MicroTable",
//                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_ONLY);
//    }
//
//    protected void WRITE_LOCK_AHEAD(IBWJEvent event, TxnContext txnContext) throws DatabaseException {
//        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i)
//            transactionManager.lock_ahead(txnContext, "MicroTable",
//                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_WRITE);
//    }
//
//    private boolean process_request_noLock(IBWJEvent event, TxnContext txnContext, CommonMetaTypes.AccessType accessType) throws DatabaseException {
//        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
//            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "MicroTable",
//                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
//            if (rt) {
//                assert event.getRecord_refs()[i].getRecord() != null;
//            } else {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    private boolean process_request(IBWJEvent event, TxnContext txnContext, CommonMetaTypes.AccessType accessType) throws DatabaseException, InterruptedException {
//        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
//            boolean rt = transactionManager.SelectKeyRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
//            if (rt) {
//                assert event.getRecord_refs()[i].getRecord() != null;
//            } else {
//                return true;
//            }
//        }
//        return false;
//    }

//    protected boolean read_request_noLock(IBWJEvent event, TxnContext txnContext) throws DatabaseException {
//        return !process_request_noLock(event, txnContext, READ_ONLY);
//    }
//
//    protected boolean write_request_noLock(IBWJEvent event, TxnContext txnContext) throws DatabaseException {
//        return !process_request_noLock(event, txnContext, READ_WRITE);
//    }
//
//    protected boolean read_request(IBWJEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
//        return !process_request(event, txnContext, READ_ONLY);
//    }
//
//    protected boolean write_request(IBWJEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
//        return !process_request(event, txnContext, READ_WRITE);
//    }

    //lock_ratio-ahead phase.
//    protected void LAL_PROCESS(double _bid) throws DatabaseException, InterruptedException {
//        //ONLY USED BY LAL, LWM, and PAT.
//    }
//
//    //post stream processing phase..
//    protected void POST_PROCESS(double _bid, long timestamp, int combo_bid_size) throws InterruptedException {
//        BEGIN_POST_TIME_MEASURE(thread_Id);
//        for (double i = _bid; i < _bid + combo_bid_size; i++) {
//            IBWJEvent event = (IBWJEvent) input_event;
//            (event).setTimestamp(timestamp);
//            boolean flag = event.READ_EVENT();
//            if (flag) {//read
//                READ_POST(event);
//            } else {
//                WRITE_POST(event);
//            }
//        }
//        END_POST_TIME_MEASURE(thread_Id);
//    }
}
