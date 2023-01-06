package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import common.param.ed.wu.WUEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import utils.AppConfig;
import utils.lib.ConcurrentHashMap;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;

public abstract class TRBolt extends TransactionalBolt {

    SINKCombo sink;

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

//        BEGIN_ACCESS_TIME_MEASURE(thread_Id); //TODO: Do we need this measure?

        AppConfig.randomDelay();

        List<DataBox> tweetValues = event.tweetRecordRef.getRecord().getValues();
        tweetValues.get(1).setStringList(Arrays.asList(event.getWords()));
        collector.force_emit(event.getBid(), null, event.getTimestamp()); //TODO: Check this emit method

//        END_ACCESS_TIME_MEASURE_ACC(thread_Id); //TODO: Do we need this measure?

    }

    protected void TWEET_REGISTRANT_REQUEST_POST(TREvent event) throws InterruptedException {

        String tweetID = event.getTweetID();
        String[] words = event.getWords();
        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;

        for (String word : words) {

            String wordID = String.valueOf(Math.abs(word.hashCode()) % 10007 % 30000);
            WUEvent outEvent = new WUEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(),
                    word, wordID, tweetID);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

//            LOG.info("Posting event: " + event.getBid());

            if (!enable_app_combo) {
                collector.emit(outBid, tuple);
            } else {
                if (enable_latency_measurement) {
                    sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
                }
            }
        }
    }

    protected void EMIT_STOP_SIGNAL(TREvent event) throws InterruptedException {

        String tweetID = event.getTweetID();
        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;

        for (int i=0; i<tthread; i++) { //send stop signal to all threads

            WUEvent outEvent = new WUEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(),
                    "Stop", "Stop", tweetID);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

//            LOG.info("Sending stop signal to downstream: " + outBid);

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
