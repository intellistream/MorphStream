package common.bolts.transactional.ed.sc;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.sc.SCEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import transaction.context.TxnContext;
import utils.AppConfig;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public class SCBolt extends TransactionalBolt {
    SINKCombo sink;
    static AtomicInteger scPostCount = new AtomicInteger(0);
    static AtomicInteger scStopCount = new AtomicInteger(0);
    static ConcurrentSkipListSet<Integer> scPostTweets = new ConcurrentSkipListSet<>();
    static ConcurrentSkipListSet<Double> scPostEvents = new ConcurrentSkipListSet<>();


    public SCBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_sc";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    //Used in: nocc
    protected void SIMILARITY_CALCULATE_REQUEST(SCEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
//        transactionManager.SelectKeyRecord(txnContext, "word_table", event.getWordID(), event.wordRecordRef, READ_WRITE);
//        assert event.wordRecordRef.getRecord() != null;
    }

    //Used in: nocc //TODO: Add version control
    protected void SIMILARITY_CALCULATE_REQUEST_CORE(SCEvent event) {

//        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        AppConfig.randomDelay();



//        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    //Handling the CORE method as in TCBolt_ts, pass updated record to event
    protected void CORE_PROCESS() {

    }

    //post stream processing phase.. nocc,
    protected void POST_PROCESS(double _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            ((SCEvent) input_event).setTimestamp(timestamp);
            SIMILARITY_CALCULATE_REQUEST_POST((SCEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }


    protected void SIMILARITY_CALCULATE_REQUEST_POST(SCEvent event) throws InterruptedException {

        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;
        String tweetID = event.getTweetID();
        String targetClusterID = event.targetClusterID;

        if (outBid >= total_events) { //Label stopping signals, for testing
            targetClusterID = "Stop";
            if (scStopCount.incrementAndGet() == tthread*tthread) {
                LOG.info("All stop signals are detected, posted tweets: " + scPostTweets);
                LOG.info("All stop signals are detected, posted events: " + scPostEvents);
            }
//            LOG.info("Thread " + thread_Id + " is posting stop signal " + outBid);
        }
        else {
            scPostCount.incrementAndGet();
            scPostTweets.add(Integer.parseInt(tweetID));
        }
        scPostEvents.add(outBid);

        if (targetClusterID == null) { //SC fail to find the most similar cluster
            throw new NullPointerException();
        }

        CUEvent outEvent = new CUEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), tweetID, targetClusterID);
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
        Tuple tuple = new Tuple(outEvent.getMyBid(), 0, context, generalMsg);

//        LOG.info("Thread " + thread_Id + " is posting event: " + outBid);

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
