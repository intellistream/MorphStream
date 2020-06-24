package application.bolts.transactional.gs;

import application.param.mb.MicroEvent;
import application.sink.SINKCombo;
import org.slf4j.Logger;
import sesame.components.operators.api.TransactionalBolt;
import sesame.execution.runtime.tuple.impl.Tuple;
import sesame.execution.runtime.tuple.impl.msgs.GeneralMsg;
import state_engine.DatabaseException;
import state_engine.Meta.MetaTypes;
import state_engine.storage.SchemaRecord;
import state_engine.storage.SchemaRecordRef;
import state_engine.storage.datatype.DataBox;
import state_engine.transaction.impl.TxnContext;

import java.util.List;

import static application.CONTROL.*;
import static application.Constants.DEFAULT_STREAM_ID;
import static application.constants.GrepSumConstants.Constant.VALUE_LEN;
import static state_engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static state_engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static state_engine.profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static state_engine.profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class GSBolt extends TransactionalBolt {
    public SINKCombo sink;

    public GSBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "gs";
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {

    }

    protected boolean READ_CORE(MicroEvent event) {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];

            if (ref.isEmpty())
                return false;//not yet processed.

            DataBox dataBox = ref.getRecord().getValues().get(1);
            int read_result = Integer.parseInt(dataBox.getString().trim());
            event.result[i] = read_result;
        }
        return true;
    }

//    volatile int com_result = 0;

    protected void READ_POST(MicroEvent event) throws InterruptedException {
        int sum = 0;
        if (POST_COMPUTE_COMPLEXITY != 0) {
            for (int i = 0; i < NUM_ACCESSES; ++i) {
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
                    sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, sum, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
                }
            }
        }
        sum = 0;
    }

    protected void WRITE_POST(MicroEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {

            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }


    protected void WRITE_CORE(MicroEvent event) {
//        long start = System.nanoTime();
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            List<DataBox> values = event.getValues()[i];
            SchemaRecordRef recordRef = event.getRecord_refs()[i];
            SchemaRecord record = recordRef.getRecord();
            List<DataBox> recordValues = record.getValues();
            recordValues.get(1).setString(values.get(1).getString(), VALUE_LEN);
        }

    }


    protected void READ_LOCK_AHEAD(MicroEvent Event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_ONLY);
    }


    protected void WRITE_LOCK_AHEAD(MicroEvent Event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_WRITE);
    }

    private boolean process_request_noLock(MicroEvent event, TxnContext txnContext, MetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean process_request(MicroEvent event, TxnContext txnContext, MetaTypes.AccessType accessType) throws DatabaseException, InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    protected boolean read_request_noLock(MicroEvent event, TxnContext txnContext) throws DatabaseException {

        return !process_request_noLock(event, txnContext, READ_ONLY);
    }


    protected boolean write_request_noLock(MicroEvent event, TxnContext txnContext) throws DatabaseException {

        return !process_request_noLock(event, txnContext, READ_WRITE);
    }

    protected boolean read_request(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        return !process_request(event, txnContext, READ_ONLY);
    }


    protected boolean write_request(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        return !process_request(event, txnContext, READ_WRITE);
    }


    //lock_ratio-ahead phase.
    protected void LAL_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        //ONLY USED BY LAL, LWM, and PAT.
    }


    //post stream processing phase..
    protected void POST_PROCESS(long _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            MicroEvent event = (MicroEvent) input_event;
            (event).setTimestamp(timestamp);
            boolean flag = event.READ_EVENT();
            if (flag) {//read
                READ_POST(event);
            } else {
                WRITE_POST(event);
            }
        }
        END_POST_TIME_MEASURE(thread_Id);
    }


}
