package common.bolts.transactional.tp;

import common.datatype.TollNotification;
import common.datatype.util.AvgValue;
import common.datatype.util.SegmentIdentifier;
import common.param.lr.LREvent;
import common.sink.SINKCombo;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import storage.datatype.DataBox;
import transaction.impl.TxnContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;
import static common.meta.MetaTypes.AccessType.READ_WRITE;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class TPBolt extends TransactionalBolt {
    /**
     * Maps each vehicle to its average speed value that corresponds to the current 'minute number' and specified segment.
     */
    private final Map<Integer, Pair<AvgValue, SegmentIdentifier>> avgSpeedsMap = new HashMap<>();
    /**
     * The currently processed 'minute number'.
     */
    private final short currentMinute = 1;
    private final short time = -1;//not in use.
    SINKCombo sink;
    TollNotification tollNotification;
    Tuple tuple;

    public TPBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "tptxn";
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected void TXN_REQUEST_NOLOCK(LREvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.SelectKeyRecord_noLock(txnContext, "segment_speed"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.speed_value//holder to be filled up.
                , READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.count_value//holder to be filled up.
                , READ_WRITE);
    }

    protected void TXN_REQUEST(LREvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        transactionManager.SelectKeyRecord(txnContext, "segment_speed"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.speed_value//holder to be filled up.
                , READ_WRITE);
        transactionManager.SelectKeyRecord(txnContext, "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.count_value//holder to be filled up.
                , READ_WRITE);
    }

    protected void REQUEST_LOCK_AHEAD(LREvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.lock_ahead(txnContext, "segment_speed", String.valueOf(event.getPOSReport().getSegment()), event.speed_value, READ_WRITE);
        transactionManager.lock_ahead(txnContext, "segment_cnt", String.valueOf(event.getPOSReport().getSegment()), event.count_value, READ_WRITE);
    }

    TollNotification toll_process(Integer vid, Integer count, Double lav, short time) {
        int toll = 0;
        if (lav < 40) {
            if (count > 50) {
                //TODO: check accident. not in use in this experiment.
                { // only true if no accident was found and "break" was not executed
                    final int var = count - 50;
                    toll = 2 * var * var;
                }
            }
        }
        // TODO GetAndUpdate accurate emit time...
//        return new TollNotification(
//                time, time, vid, lav, toll);
        return null;
    }

    void REQUEST_POST(LREvent event) throws InterruptedException {
        tollNotification = toll_process(event.getPOSReport().getVid(), event.count, event.lav, event.getPOSReport().getTime());
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement)
                tuple = new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, tollNotification, event.getTimestamp()));
            else
                tuple = null;
            sink.execute(tuple);
        }
    }

    @Override
    protected void POST_PROCESS(long bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            LREvent event = (LREvent) input_event;
            ((LREvent) input_event).setTimestamp(timestamp);
            REQUEST_POST(event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected void TXN_REQUEST_CORE(LREvent event) {
        DataBox dataBox = event.count_value.getRecord().getValues().get(1);
        HashSet cnt_segment = dataBox.getHashSet();
        cnt_segment.add(event.getPOSReport().getVid());//update hashset; updated state also. TODO: be careful of this.
        event.count = cnt_segment.size();
        DataBox dataBox1 = event.speed_value.getRecord().getValues().get(1);
        event.lav = dataBox1.getDouble();
    }
}

