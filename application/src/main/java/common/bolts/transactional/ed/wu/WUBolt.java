package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import common.param.ed.tc.TCEvent;
import common.param.ed.wu.WUEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import utils.AppConfig;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public class WUBolt extends TransactionalBolt {
    SINKCombo sink;

    public WUBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_wu";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    //Used in: nocc
    protected void WORD_UPDATE_REQUEST(WUEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        transactionManager.SelectKeyRecord(txnContext, "word_table", event.getWordID(), event.wordRecordRef, READ_WRITE);
        assert event.wordRecordRef.getRecord() != null;
    }

    //Used in: nocc //TODO: Add version control
    protected void WORD_UPDATE_REQUEST_CORE(WUEvent event) {

//        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        AppConfig.randomDelay();

        List<DataBox> wordValues = event.wordRecordRef.getRecord().getValues();
        if (wordValues == null) {
            LOG.info("TC: Word record not found");
            throw new NoSuchElementException();
        }

        final long oldCountOccurWindow = wordValues.get(3).getLong();

        if (oldCountOccurWindow != 0) { // word record is not empty
            final int oldLastOccurWindow = wordValues.get(5).getInt();
            final long oldFrequency = wordValues.get(6).getLong();

            // Update word's tweetList
            wordValues.get(2).addItem(event.getTweetID()); //append new tweetID into word's tweetList

            // Update word's window info
            if (oldLastOccurWindow < event.getCurrWindow()) { //oldLastOccurWindow less than currentWindow
                wordValues.get(3).incLong(oldCountOccurWindow, 1); //countOccurWindow += 1
                wordValues.get(5).setInt(event.getCurrWindow()); //update lastOccurWindow to currentWindow
            }
            // Update word's inner-window frequency
            wordValues.get(6).incLong(oldFrequency, 1); //frequency += 1

        } else { // word record is empty
            String[] tweetList = {event.getTweetID()};
            wordValues.get(1).setString(event.getWord()); //wordValue
            wordValues.get(2).setStringList(Arrays.asList(tweetList)); //tweetList
            wordValues.get(3).setLong(1); //countOccurWindow
            wordValues.get(5).setInt(event.getCurrWindow()); //lastOccurWindow
            wordValues.get(6).setLong(1); //frequency
        }

//        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    //post stream processing phase.. nocc,
    protected void POST_PROCESS(double _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            ((WUEvent) input_event).setTimestamp(timestamp);
            WORD_UPDATE_REQUEST_POST((WUEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    static AtomicInteger threadPostCount = new AtomicInteger(0);

    protected void WORD_UPDATE_REQUEST_POST(WUEvent event) throws InterruptedException {

        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;
        String tweetID = event.getTweetID();

        if (!enable_app_combo) {
            String wordID = event.getWordID();

            TCEvent outEvent = new TCEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), wordID, tweetID);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

//            LOG.info("Thread " + thread_Id + " posting event: " + event.getBid());

            collector.emit(outBid, tuple);//tuple should be the input of next bolt's execute() method
//            LOG.info("Threads " + thread_Id + " posted event count: " + threadPostCount.incrementAndGet());

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }
}
