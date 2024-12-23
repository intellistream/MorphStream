package common.bolts.transactional.gsw;

import combo.SINKCombo;
import common.param.gsw.WindowedMicroEvent;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.READ_ONLY;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class GSWBolt extends TransactionalBolt {
    public SINKCombo sink;

    public ConcurrentHashMap<String, ConcurrentSkipListMap<Long, SchemaRecord>> windowMap;

    public GSWBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "gs";
        this.windowMap = new ConcurrentHashMap<>();
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected boolean READ_CORE(WindowedMicroEvent event) {
        long sum = 0;

        // apply function
        AppConfig.randomDelay();

        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];
            if (ref.isEmpty()) {
                return false;//not yet processed.
            }

            DataBox keyBox = ref.getRecord().getValues().get(0);
            List<SchemaRecord> schemaRecordRange = readPreValuesRange(windowMap.computeIfAbsent(
                            keyBox.getString().trim(), k -> new ConcurrentSkipListMap<>()),
                    event.getBid(), AppConfig.windowSize);
            sum += schemaRecordRange.stream().mapToLong(schemaRecord -> schemaRecord.getValues().get(1).getLong()).sum();


            DataBox dataBox = ref.getRecord().getValues().get(1);
            long read_result = Long.parseLong(dataBox.getString().trim());
            event.result.add(read_result);
        }
        return true;
    }

    public List<SchemaRecord> readPreValuesRange(ConcurrentSkipListMap<Long, SchemaRecord> window, long ts, long range) {
        long start = ts - range < 0 ? 0 : ts - range;
        ConcurrentNavigableMap<Long, SchemaRecord> schemaRange = window.tailMap(start);

        //not modified in last round
        if (schemaRange.size() == 0)
            System.out.println("Empty window");
        else
            System.out.println(schemaRange.size());

        return new ArrayList<>(schemaRange.values());
    }

    //    volatile int com_result = 0;
    protected void READ_POST(WindowedMicroEvent event) throws InterruptedException {
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

    protected void WRITE_POST(WindowedMicroEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }

    protected void WRITE_CORE(WindowedMicroEvent event) {
//        long start = System.nanoTime();
        long sum = 0;
        SchemaRecordRef ref = event.getRecord_refs()[0];

        // insert the write records into the window state
        DataBox keyBox = ref.getRecord().getValues().get(0);
        ConcurrentSkipListMap<Long, SchemaRecord> curWindow = windowMap.computeIfAbsent(keyBox.getString().trim(), k -> new ConcurrentSkipListMap<>());
        curWindow.put(event.getBid(), ref.getRecord());

        DataBox TargetValue_value = ref.getRecord().getValues().get(1);

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

    protected void READ_LOCK_AHEAD(WindowedMicroEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_ONLY);
    }

    protected void WRITE_LOCK_AHEAD(WindowedMicroEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_WRITE);
    }

    private boolean process_request_noLock(WindowedMicroEvent event, TxnContext txnContext, CommonMetaTypes.AccessType accessType) throws DatabaseException {
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

    private boolean process_request(WindowedMicroEvent event, TxnContext txnContext, CommonMetaTypes.AccessType accessType) throws DatabaseException, InterruptedException {
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

    protected boolean read_request_noLock(WindowedMicroEvent event, TxnContext txnContext) throws DatabaseException {
        return !process_request_noLock(event, txnContext, READ_ONLY);
    }

    protected boolean write_request_noLock(WindowedMicroEvent event, TxnContext txnContext) throws DatabaseException {
        return !process_request_noLock(event, txnContext, READ_WRITE);
    }

    protected boolean read_request(WindowedMicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        return !process_request(event, txnContext, READ_ONLY);
    }

    protected boolean write_request(WindowedMicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
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
            WindowedMicroEvent event = (WindowedMicroEvent) input_event;
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
