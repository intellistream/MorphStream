package intellistream.morphstream.examples.tsp.grepsumwindow.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.grepsumwindow.events.GSWTxnEvent;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalBolt;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.util.AppConfig;
import org.slf4j.Logger;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.READ_ONLY;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class GSWBolt extends TransactionalBolt {
    public SINKCombo sink;

    public GSWBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "gs";
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected boolean READ_CORE(GSWTxnEvent event) {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];
            if (ref.isEmpty()) {
                return false;//not yet processed.
            }
            DataBox dataBox = ref.getRecord().getValues().get(1);
            int read_result = Integer.parseInt(dataBox.getString().trim());
            event.result.add(read_result);
        }
        return true;
    }

    //    volatile int com_result = 0;
    protected void READ_POST(GSWTxnEvent event) throws InterruptedException {
        int sum = 0;
        if (POST_COMPUTE_COMPLEXITY != 0) {
            for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
                sum += event.result.get(i);
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

    protected void WRITE_POST(GSWTxnEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }

    protected void WRITE_CORE(GSWTxnEvent event) {
//        long start = System.nanoTime();
        long sum = 0;
        DataBox TargetValue_value = event.getRecord_refs()[0].getRecord().getValues().get(1);
        int NUM_ACCESS = event.TOTAL_NUM_ACCESS / event.Txn_Length;
        for (int j = 0; j < event.Txn_Length; ++j) {
            AppConfig.randomDelay();
            for (int i = 0; i < NUM_ACCESS; ++i) {
                int offset = j * NUM_ACCESS + i;
                SchemaRecordRef recordRef = event.getRecord_refs()[offset];
                SchemaRecord record = recordRef.getRecord();
                DataBox Value_value = record.getValues().get(1);
                final long Value = Value_value.getLong();
                sum += Value;
            }
        }
        sum /= event.TOTAL_NUM_ACCESS;
        TargetValue_value.setLong(sum);
    }

    protected void READ_LOCK_AHEAD(GSWTxnEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_ONLY);
    }

    protected void WRITE_LOCK_AHEAD(GSWTxnEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_WRITE);
    }

    private boolean process_request_noLock(GSWTxnEvent event, TxnContext txnContext, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
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

    private boolean process_request(GSWTxnEvent event, TxnContext txnContext, CommonMetaTypes.AccessType accessType) throws DatabaseException, InterruptedException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
            boolean rt = transactionManager.SelectKeyRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    protected boolean read_request_noLock(GSWTxnEvent event, TxnContext txnContext) throws DatabaseException {
        return !process_request_noLock(event, txnContext, READ_ONLY);
    }

    protected boolean write_request_noLock(GSWTxnEvent event, TxnContext txnContext) throws DatabaseException {
        return !process_request_noLock(event, txnContext, READ_WRITE);
    }

    protected boolean read_request(GSWTxnEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        return !process_request(event, txnContext, READ_ONLY);
    }

    protected boolean write_request(GSWTxnEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
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
            GSWTxnEvent event = (GSWTxnEvent) input_event;
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
