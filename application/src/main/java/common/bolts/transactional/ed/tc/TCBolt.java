package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import common.param.ed.tc.TCEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import utils.AppConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public class TCBolt extends TransactionalBolt {
    SINKCombo sink;

    public TCBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tc";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    //Used in: nocc
    protected void TREND_CALCULATE_REQUEST(TCEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        transactionManager.SelectKeyRecord(txnContext, "word_table", event.getWordID(), event.wordRecordRef, READ_WRITE);
        assert event.wordRecordRef.getRecord() != null;
    }

    //Used in: nocc
    protected void TREND_CALCULATE_REQUEST_CORE(TCEvent event) {

//        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        AppConfig.randomDelay();

        List<DataBox> wordValues = event.wordRecordRef.getRecord().getValues();
        if (wordValues == null) {
            LOG.info("TC: Word record not found");
            throw new NoSuchElementException();
        }
        final long oldCountOccurWindow = wordValues.get(3).getLong();
        final double oldTfIdf = wordValues.get(4).getDouble();
        final long oldFrequency = wordValues.get(6).getLong();

        // Compute word's tf-idf
        int windowSize = event.getWindowSize();
        int windowCount = event.getWindowCount();
        double tf = (double) oldFrequency / windowSize;
        double idf = -1 * (Math.log((double) oldCountOccurWindow / windowCount));
        double newTfIdf = tf * idf;
        double difference = tf * idf - oldTfIdf;

        List<String> emptyList = new ArrayList<>();
        wordValues.get(2).setStringList(emptyList); //reset tweetIDList to {}
        wordValues.get(4).setDouble(newTfIdf); //update tf-idf
        wordValues.get(6).setLong(0); //reset frequency to zero
        wordValues.get(7).setBool(difference >= 0.5); //set isBurst accordingly //TODO: Check this threshold

//        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    //Used on nocc: Handling the CORE method as in TCBolt_ts, pass updated record to event
    protected void CORE_PROCESS() {
        TCEvent event = (TCEvent) input_event;
        SchemaRecordRef ref = event.wordRecordRef;
        if (ref.isEmpty()) {
            LOG.info("Thead " + thread_Id + " reads empty word record");
            throw new NullPointerException();
        }
        try {
            event.tweetIDList = ref.getRecord().getValues().get(2).getStringList().toArray(new String[0]).clone();
            event.isBurst = ref.getRecord().getValues().get(7).getBool();
            for (String tweetID : event.tweetIDList) {
                if (tweetID == null) {
                    LOG.info("Null value found in array");
                }
            }
        } catch (Exception e) {
            LOG.info("ArrayIndexOutOfBounds");
        }

    }

    //post stream processing phase.. nocc,
    protected void POST_PROCESS(double _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            ((TCEvent) input_event).setTimestamp(timestamp);
            TREND_CALCULATE_REQUEST_POST((TCEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected void TREND_CALCULATE_REQUEST_POST(TCEvent event) throws InterruptedException {

        double outBid = Math.round(event.getMyBid() * 10.0) / 10.0;

        if (!enable_app_combo) {
//            LOG.info("Thread " + thread_Id + " posting event: " + outBid);

            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, event.getTimestamp());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

            collector.emit(outBid, tuple); //emit to TCG
//            LOG.info("Threads " + thread_Id + " posted event count: " + threadPostCount.incrementAndGet());

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }

}
