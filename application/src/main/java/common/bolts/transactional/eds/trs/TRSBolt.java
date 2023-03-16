package common.bolts.transactional.eds.trs;

import combo.SINKCombo;
import common.param.eds.trs.TRSEvent;
import common.param.eds.wus.WUSEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import java.util.NoSuchElementException;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public class TRSBolt extends TransactionalBolt {

    SINKCombo sink;

    public TRSBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "eds_trs";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    //Used in all algorithms
    protected void TWEET_REGISTRANT_REQUEST_POST(TRSEvent event) throws InterruptedException {

        String tweetID = event.getTweetID();
        if (tweetID == null) {
            LOG.info("Null tweetID found in event");
            throw new NoSuchElementException();
        }
        String[] words = event.getWords();
        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;

        for (String word : words) {

            String wordID = String.valueOf(Math.abs(word.hashCode()) % 10007 % 30000);
            WUSEvent outEvent = new WUSEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(),
                    word, wordID, tweetID);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

//            LOG.info("Thread " + thread_Id + " posting event: " + event.getBid());

            if (!enable_app_combo) {
                collector.emit(outBid, tuple);
//                LOG.info("Threads " + thread_Id + " posted event count: " + threadPostCount.incrementAndGet());
            } else {
                if (enable_latency_measurement) {
                    sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
                }
            }
        }
    }

    //This is only used for MorphStream/TStream for punctuation control
    protected void EMIT_STOP_SIGNAL(TRSEvent event) throws InterruptedException {

        String tweetID = event.getTweetID();
        if (tweetID == null) {
            LOG.info("Null tweetID found in event");
            throw new NoSuchElementException();
        }
        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;

        for (int i=0; i<tthread; i++) { //send stop signal to all threads

            WUSEvent outEvent = new WUSEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(),
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
