package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import common.param.ed.wu.WUEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import scala.App;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import utils.AppConfig;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class TRBolt extends TransactionalBolt {

    SINKCombo sink;
    ConcurrentHashMap<String, String> wordHashMap = new ConcurrentHashMap<>();
    CopyOnWriteArrayList<String> conflictWords = new CopyOnWriteArrayList<>();

    public TRBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tr";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    //Used in: nocc
    protected void TWEET_REGISTRANT_REQUEST(TREvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        transactionManager.SelectKeyRecord(txnContext, "tweet_table", event.getTweetID(), event.tweetRecordRef, READ_WRITE);
        assert event.tweetRecordRef.getRecord() != null;
    }

    //Used in: nocc
    protected void TWEET_REGISTRANT_REQUEST_CORE(TREvent event) throws InterruptedException {
//        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//        AppConfig.randomDelay();
        List<DataBox> tweetValues = event.tweetRecordRef.getRecord().getValues();
        if (tweetValues == null) {
            LOG.info("Tweet record not found");
            throw new NoSuchElementException();
        }
        tweetValues.get(1).setStringList(Arrays.asList(event.getWords()));
//        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    //post stream processing phase.. for nocc, lwm and sstore
    protected void POST_PROCESS(double _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            ((TREvent) input_event).setTimestamp(timestamp);
            TWEET_REGISTRANT_REQUEST_POST((TREvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    //Used in all algorithms
    protected void TWEET_REGISTRANT_REQUEST_POST(TREvent event) throws InterruptedException {

        String tweetID = event.getTweetID();
        if (tweetID == null) {
            LOG.info("Null tweetID found in event");
            throw new NoSuchElementException();
        }
        String[] words = event.getWords();
        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;

        for (String word : words) {
//            String wordID = String.valueOf(Math.abs(word.hashCode()) % 29989 % 50000);
            String wordID = AppConfig.wordToIndexMap.get(word);
            if (wordID != null) {
                WUEvent outEvent = new WUEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(),
                        word, wordID, tweetID);
                Tuple tuple = new Tuple(outBid, 0, context, new GeneralMsg<>(DEFAULT_STREAM_ID, outEvent, event.getTimestamp()));
//                LOG.info("Thread " + thread_Id + " posting event: " + event.getBid());
                if (!enable_app_combo) {
                    collector.emit(outBid, tuple);
//                LOG.info("Threads " + thread_Id + " posted event count: " + threadPostCount.incrementAndGet());
                } else {
                    if (enable_latency_measurement) {
                        sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
                    }
                }
            } else {
                LOG.info("No matching index found for word: " + word);
            }

        }
    }

    //This is only used for MorphStream/TStream for punctuation control
    protected void EMIT_STOP_SIGNAL(TREvent event) throws InterruptedException {

        String tweetID = event.getTweetID();
        if (tweetID == null) {
            LOG.info("Null tweetID found in event");
            throw new NoSuchElementException();
        }
        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;

        for (int i=0; i<tthread; i++) { //send stop signal to all threads

            WUEvent outEvent = new WUEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(),
                    "Stop", "Stop", tweetID);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);
//            LOG.info("Thread " + thread_Id + " sending stop signal: " + outBid);

            if (!enable_app_combo) {
                collector.emit(outBid, tuple);
            } else {
                if (enable_latency_measurement) {
                    sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
                }
            }
        }
    }

}
